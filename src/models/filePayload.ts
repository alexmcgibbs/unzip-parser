import { getDbPool } from "../db";
import { parseFilePayload, PersistenceSummary } from "../types";

export async function persistWebhookPayload(fileName: string, payload: unknown): Promise<PersistenceSummary> {
  const parsed = parseFilePayload(payload);
  const pool = getDbPool();
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const fileInsertResult = await client.query<{ id: number }>(
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
        updated_at = NOW()
      RETURNING id`,
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

    const fileId = fileInsertResult.rows[0].id;

    await client.query("DELETE FROM accounts WHERE file_id = $1", [fileId]);

    let accountsInserted = 0;
    let holdingsInserted = 0;

    for (const account of parsed.accounts) {
      const accountInsertResult = await client.query<{ id: number }>(
        `INSERT INTO accounts (
          file_id,
          account_id,
          account_type,
          custodian,
          opened_date,
          status,
          cash_balance,
          total_value
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id`,
        [
          fileId,
          account.account_id,
          account.account_type,
          account.custodian,
          account.opened_date,
          account.status,
          account.cash_balance,
          account.total_value
        ]
      );

      const accountRowId = accountInsertResult.rows[0].id;
      accountsInserted += 1;

      for (const holding of account.holdings) {
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
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
          [
            accountRowId,
            holding.ticker,
            holding.cusip,
            holding.description,
            holding.quantity,
            holding.market_value,
            holding.cost_basis,
            holding.price,
            holding.asset_class
          ]
        );
        holdingsInserted += 1;
      }
    }

    await client.query("COMMIT");

    return { fileId, accountsInserted, holdingsInserted };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}
