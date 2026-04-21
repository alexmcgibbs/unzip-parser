import { getDbPool } from "../db";
import { parseFilePayload, PersistenceSummary } from "../types";

export async function persistWebhookPayload(
  fileName: string,
  payload: unknown,
  jobId: string
): Promise<PersistenceSummary> {
  const payloadItems = Array.isArray(payload) ? payload : [payload];
  if (payloadItems.length === 0) {
    throw new Error("json payload array must contain at least one item.");
  }

  const pool = getDbPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    let accountsInserted = 0;
    let holdingsInserted = 0;
    let lastClientId = "";

    for (const item of payloadItems) {
      const parsed = parseFilePayload(item);
      lastClientId = parsed.client_id;

      await client.query(
        `INSERT INTO files (
          file_name,
          job_id,
          client_id,
          first_name,
          last_name,
          email,
          advisor_id,
          last_updated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (client_id)
        DO UPDATE SET
          file_name = EXCLUDED.file_name,
          job_id = EXCLUDED.job_id,
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          email = EXCLUDED.email,
          advisor_id = EXCLUDED.advisor_id,
          last_updated = EXCLUDED.last_updated,
          updated_at = NOW()`,
        [
          fileName,
          jobId,
          parsed.client_id,
          parsed.first_name,
          parsed.last_name,
          parsed.email,
          parsed.advisor_id,
          parsed.last_updated
        ]
      );

      await client.query("DELETE FROM accounts WHERE client_id = $1", [parsed.client_id]);

      for (const account of parsed.accounts) {
        await client.query(
          `INSERT INTO accounts (
            client_id,
            account_id,
            account_type,
            custodian,
            opened_date,
            status,
            cash_balance,
            total_value
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
          ON CONFLICT (account_id)
          DO UPDATE SET
            client_id = EXCLUDED.client_id,
            account_type = EXCLUDED.account_type,
            custodian = EXCLUDED.custodian,
            opened_date = EXCLUDED.opened_date,
            status = EXCLUDED.status,
            cash_balance = EXCLUDED.cash_balance,
            total_value = EXCLUDED.total_value,
            updated_at = NOW()`,
          [
            parsed.client_id,
            account.account_id,
            account.account_type,
            account.custodian,
            account.opened_date,
            account.status,
            account.cash_balance,
            account.total_value
          ]
        );

        accountsInserted += 1;

        if (account.holdings.length > 0) {
          await client.query(
            `INSERT INTO holdings (
              account_id,
              ticker,
              cusip,
              description,
              quantity,
              market_value,
              cost_basis,
              price,
              asset_class
            )
            SELECT
              $1,
              unnest($2::text[]),
              unnest($3::text[]),
              unnest($4::text[]),
              unnest($5::numeric[]),
              unnest($6::numeric[]),
              unnest($7::numeric[]),
              unnest($8::numeric[]),
              unnest($9::text[])`,
            [
              account.account_id,
              account.holdings.map((h) => h.ticker),
              account.holdings.map((h) => h.cusip),
              account.holdings.map((h) => h.description),
              account.holdings.map((h) => h.quantity),
              account.holdings.map((h) => h.market_value),
              account.holdings.map((h) => h.cost_basis),
              account.holdings.map((h) => h.price),
              account.holdings.map((h) => h.asset_class)
            ]
          );
          holdingsInserted += account.holdings.length;
        }
      }
    }

    await client.query("COMMIT");

    return { clientId: lastClientId, accountsInserted, holdingsInserted };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
