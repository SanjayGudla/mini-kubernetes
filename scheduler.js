import { jobDispatchScheduler, jobCriScheduler } from './queues/queues.js'
import { jobDispatchWorker, jobCriWorker } from './queues/workers.js'

async function init() {
    Promise.all(
        [
            jobDispatchScheduler.upsertJobScheduler('job-dispatch-scheduler', {
                every: 2 * 1000,
            }),
            jobCriScheduler.upsertJobScheduler('job-cri-scheduler', {
                every: 10 * 1000,
            })
        ]
    )
}

init();