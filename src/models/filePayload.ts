import { getDbPool } from "../db";
import { parseFilePayload, PersistenceSummary } from "../types";

export async function persistWebhookPayload(fileName: string, payload: unknown): Promise<PersistenceSummary> {
  const parsed = parseFilePayload(payload);
  const pool = getDbPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    await client.query(
      `INSERT INTO files (
        file_name,
        client_id,
        first_name,
        last_name,
        email,
        advisor_id,
        last_updated
      ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (client_id)
      DO UPDATE SET
        file_name = EXCLUDED.file_name,
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        email = EXCLUDED.email,
        advisor_id = EXCLUDED.advisor_id,
        last_updated = EXCLUDED.last_updated,
        updated_at = NOW()`,
      [
        fileName,
        parsed.client_id,
        parsed.first_name,
        parsed.last_name,
        parsed.email,
        parsed.advisor_id,
        parsed.last_updated
      ]
    );

    await client.query("DELETE FROM accounts WHERE client_id = $1", [parsed.client_id]);

    let accountsInserted = 0;
    let holdingsInserted = 0;

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

    await client.query("COMMIT");

    return { clientId: parsed.client_id, accountsInserted, holdingsInserted };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
