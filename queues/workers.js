import { Worker } from 'bullmq'
import { db } from "../db/index.js"
import { jobsTable, jobStatusEnumValues } from "../db/schema.js"

import { inArray, sql } from 'drizzle-orm'

export const jobDispatchWorker = new Worker('job-dispatcher', async () => {
    console.log("[JobDispatcher]: Checking For New Submitted Jobs ...")
    await db.transaction(async (tx) => {
        const stmt = sql`
            SELECT
                id
            FROM ${jobsTable}
            WHERE
                ${jobsTable.state} = ${jobStatusEnumValues[0]}
            ORDER BY ${jobsTable.createdAt} ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 5
        `;
        const result = await tx.execute(stmt);
        const jobIds = result.rows.map((e) => e.id);

        console.log(`[JobDispatcher]: Found ${jobIds.length} jobs in Submitted State`, jobIds);

        // TODO: Check if compute is available
        if (jobIds.length > 0) {
            console.log(`[JobDispatcher]: Moving ${jobIds.length} jobs to Runnable State`);
            await tx
                .update(jobsTable)
                .set({ state: 'RUNNABLE' })
                .where(inArray(jobsTable.id, jobIds));
        }

    })
},
    {
        connection: {
            host: '127.0.0.1',
            port: 6379
        }
    }
);

export const jobCriWorker = new Worker('job-cri', async () => {

    console.log("[JobCriWorker]: Checking For New Runnable Jobs ...")
    await db.transaction(async (tx) => {
        const stmt = sql`
            SELECT
                id
            FROM ${jobsTable}
            WHERE
                ${jobsTable.state} = ${jobStatusEnumValues[1]}
            ORDER BY ${jobsTable.createdAt} ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        `;
        const result = await tx.execute(stmt);
        const jobIds = result.rows.map((e) => e.id);

        console.log(`[JobCriWorker]: Found ${jobIds.length} jobs in Runnable State`, jobIds);

        // TODO: Check if compute is available
    }
    )

}, {
    connection: {
        host: '127.0.0.1',
        port: 6379
    }
}
);
