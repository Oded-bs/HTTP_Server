#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "threadpool.h"

/**Error Function: gets: int - cause of the error, pointers to the threadpool, and the threads tehmself. will free those pointers if not NULL
 * error format is perror. the cause options are as follows: 1 - Mutex Init, 2-Condition_Init, 3-Memory_Allocation, 4-Pthread_Create
*/
void errorFunc(int cause,threadpool *tp,pthread_t* threads)
{
    if(tp)
    {
        free(tp);
    }
    if(threads)
    {
        free(threads);
    }
    switch (cause)
    {
    case 1:
        perror("Pthread_Mutex_Init\n");
        break;
    case 2:
        perror("Pthread_Cond_Init\n");
        break;
    case 3:
        perror("Allocation Failed\n");
        break;
    case 4:
        perror("Pthread_Create\n");
        break;
    }
    return;
}
/**Checks for the user input validity, valid input must be positive numbers*/
int inputIsValid(int arguments_amount,char* argv[])
{
    if(arguments_amount != 4)
    {
        printf("Usage: pool <pool-size> <number-of-tasks> <max-number-of-request>\n");
        return 0;
    }
    int pool_size = atoi(argv[1]);
    int number_of_tasks = atoi(argv[2]);
    if(pool_size < 1 || pool_size > MAXT_IN_POOL || number_of_tasks < 0)
    {
        printf("Usage: pool <pool-size> <number-of-tasks> <max-number-of-request>\n");
        return 0;
    }
    return 1;
}
/**Function that Init the threadpool and its fields, program will free Memory/Pthreads variables on error*/
threadpool* create_threadpool(int num_threads_in_pool)
{
    threadpool* tp = (threadpool*)calloc(1,sizeof(threadpool));
    if(!tp)
    {
        errorFunc(3,NULL,NULL);
        exit(EXIT_FAILURE);
    }
    tp->num_threads = num_threads_in_pool;
    tp->qsize = 0;
    tp->threads = (pthread_t*)calloc(num_threads_in_pool,sizeof(pthread_t));
    if(!tp->threads)
    {
        errorFunc(3,tp,NULL);
        exit(EXIT_FAILURE);
    }
    tp->qhead = tp->qtail = NULL;
    if(pthread_mutex_init(&tp->qlock,NULL))
    {
        errorFunc(1,tp,tp->threads);
        exit(EXIT_FAILURE);
    }
    if(pthread_cond_init(&tp->q_not_empty,NULL))
    {
        pthread_mutex_destroy(&tp->qlock);
        errorFunc(2,tp,tp->threads);
        exit(EXIT_FAILURE);
    }

    if(pthread_cond_init(&tp->q_empty,NULL))
    {
        pthread_mutex_destroy(&tp->qlock);
        pthread_cond_destroy(&tp->q_not_empty);
        errorFunc(2,tp,tp->threads);
        exit(EXIT_FAILURE);
    }
    tp->shutdown = 0;
    tp->dont_accept =0;
    for(int i = 0;i<num_threads_in_pool;i++)
    {
        if(pthread_create(&tp->threads[i],NULL,do_work,tp))
        {
            pthread_mutex_destroy(&tp->qlock);
            pthread_cond_destroy(&tp->q_empty);
            pthread_cond_destroy(&tp->q_not_empty);
            errorFunc(4,tp,tp->threads);
            exit(EXIT_FAILURE);

        }
    }
    return tp;
}
/**Creates new task and adds it to the Queue.
 * Steps: 
 * 1. we lock the mutex
 * 2.we create the task
 * 3. we add it to the queue
 * 4. signal to the waiting threads with q_not_empty 
 * 5. unlock mutex*/
void dispatch(threadpool* from_me, dispatch_fn dispatch_to_here, void *arg)
{
    pthread_mutex_lock(&from_me->qlock);
    if(from_me->dont_accept == 1)
    {
        pthread_mutex_unlock(&from_me->qlock);
        return;
    }
    work_t *work = (work_t*)calloc(1,sizeof(work_t));
    if(!work)
    {
        pthread_mutex_destroy(&from_me->qlock);
        pthread_cond_destroy(&from_me->q_empty);
        pthread_cond_destroy(&from_me->q_not_empty);
        errorFunc(3,from_me,from_me->threads);
        exit(EXIT_FAILURE);
    }
    work->routine = dispatch_to_here;
    work->arg = arg;
    if(from_me->qsize == 0)
    {
        from_me->qhead = work;
        from_me->qtail = work;
    }else
    {
        from_me->qtail->next = work;
        from_me->qtail = work;
    }
    from_me->qsize++;
    pthread_cond_signal(&from_me->q_not_empty);
    pthread_mutex_unlock(&from_me->qlock);
    return;
}
/**Function where our pthread lives. will exit only if shutdown is activated.
 * Steps:
 * 1. Lock Mutex
 * 2. Check the Queue size, if its 0 and shutdown is off the thread will wait on q_not_empty condition.
 * 3. Check if the shutdown was activated while the thread slept, if yes we exit the thread.
 * 4. We Dequeue a task from our queue.
 * 5. Start Routine.
 * 6. Unlock Mutex
*/
void* do_work(void* p)
{
    threadpool *thread_pool = (threadpool*) p;
    while(1)
    {
        pthread_mutex_lock(&thread_pool->qlock);

        while(thread_pool->qsize == 0 && thread_pool->shutdown == 0)
        {
            pthread_cond_wait(&thread_pool->q_not_empty,&thread_pool->qlock);
        }
        if(thread_pool->shutdown == 1)
        {
            pthread_mutex_unlock(&thread_pool->qlock);
            return NULL;
        }

        work_t* current_job = thread_pool->qhead;
        thread_pool->qsize--;
        if(thread_pool->qsize == 0)
        {
            thread_pool->qhead = thread_pool->qtail = NULL;
            if(thread_pool->dont_accept)
            {
                pthread_cond_signal(&thread_pool->q_empty);
            }
        }
        else if(thread_pool->qsize > 0)
        {
            thread_pool->qhead = thread_pool-> qhead->next;
        }
        pthread_mutex_unlock(&thread_pool->qlock);
        current_job->routine(current_job->arg);
        free(current_job);
    }
}

/**Destroy our threadpool
 * Steps:
 * 1. Lock Mutex
 * 2. Activate dont_accept
 * 3. Wait until the queue is empty
 * 4. Activate Shutdown
 * 5. Waiting on all the threads to complete the tasks
 * 6. Destory all the Pthread variables and free the threadpool
*/
void destroy_threadpool(threadpool* destroyme)
{
    pthread_mutex_lock(&destroyme->qlock);

    destroyme->dont_accept = 1;

    while(destroyme->qsize>0)
        pthread_cond_wait(&destroyme->q_empty,&destroyme->qlock);
    
    destroyme->shutdown = 1;

    pthread_cond_broadcast(&destroyme->q_not_empty);
    pthread_mutex_unlock(&destroyme->qlock);
    
    for(int i = 0 ; i < destroyme->num_threads; i++)
    {
        pthread_join(destroyme->threads[i],NULL);
    }
    pthread_mutex_destroy(&destroyme->qlock);
    pthread_cond_destroy(&destroyme->q_empty);
    pthread_cond_destroy(&destroyme->q_not_empty);
    free(destroyme->threads);
    free(destroyme);
}
/**This is the function the thread is using what we call Task
 * Prints the Thread ID 1000 times.
*/
void *printme(void *arg)
{
    pthread_t tid = pthread_self();
    for (int i = 0; i<1 ; i++)
    {
        printf("Thread ID: %lu\n",tid);
        usleep(100000);
    }
    return NULL;
}

int main(int argc,char* argv[])
{
    threadpool *tp; 
    if(inputIsValid(argc,argv) == 0)
    {
        exit(1);
    }
    int pool_size = atoi(argv[1]), number_of_tasks = atoi(argv[2]),max_number_of_request = atoi(argv[3]), tasks_dispatched = 0;
    tp=create_threadpool(pool_size);
    while(1)
    {
        if(tasks_dispatched < number_of_tasks)
        {
            tasks_dispatched++;
            dispatch(tp,(void *)printme,NULL);
        }
        if(tasks_dispatched == max_number_of_request)
        {
            destroy_threadpool(tp);
            break;
        }
    }
    return 0;
}