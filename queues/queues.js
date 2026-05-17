import { Queue } from 'bullmq'

// Schedulers

export const jobDispatchScheduler = new Queue('job-dispatcher');

export const jobCriScheduler = new Queue('job-cri');
