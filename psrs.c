#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>
#include <string.h>
#include "psrs.h"

#define LIST_LEN 64
#define NUM_PROC 4
#define ROOT 0

/*MPI tags*/
#define TAG_SEG 1

/*TODO: somewhere in code, set error handler*/
/*TODO: account for non-even distributions of keys to sort*/

int
longcmp(const void * a, const void * b)
{
    const long * first = (const long *)a;
    const long * sec = (const long *)b;

    if (*first < *sec) {
        return -1;
    } else if (*first > *sec) {
        return 1;
    }

    return 0;    
}

int 
main(int argc, char * argv[])
{
    int i;
    int j;
    int rank;
    int part_size[NUM_PROC];
    int part_start[NUM_PROC];
    int rpart_size[NUM_PROC];
    int rpart_start[NUM_PROC];
    long list[LIST_LEN];
    long sbuf[LIST_LEN];
    long rbuf[LIST_LEN];
    long seg[LIST_LEN/NUM_PROC];

    int cur_rank;

    MPI_Status mpistat;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    
    /*Phase 1*/
    /*if process zero, generate and distribute segments*/
    if (rank == ROOT) {
        /*generate list to sort*/
        srandom(time(NULL));
        for (i = 0; i < LIST_LEN; i ++) {
            list[i] = random() % 10;
        }
    }  
    /*distribute segments*/
    MPI_Scatter(list, LIST_LEN/NUM_PROC, MPI_LONG,
               rbuf, LIST_LEN/NUM_PROC, MPI_LONG, ROOT, 
               MPI_COMM_WORLD);
    
    /*Sort own segment*/
    memcpy(seg, rbuf, LIST_LEN/NUM_PROC*sizeof(long));
    qsort(seg, LIST_LEN/NUM_PROC, sizeof(long), longcmp);
   
    /*choose and give ROOT samples*/
    for (i = 0; i < NUM_PROC; i ++) {
        sbuf[i] = seg[(LIST_LEN/NUM_PROC/NUM_PROC)*i];
    }
    MPI_Gather(sbuf, NUM_PROC, MPI_LONG, rbuf, NUM_PROC,
              MPI_LONG, ROOT, MPI_COMM_WORLD);
    
    /*Phase 2*/
    if (rank == ROOT) {
        long samples[NUM_PROC*NUM_PROC];
        
        /*sort list of samples*/
        memcpy(samples, rbuf, NUM_PROC*NUM_PROC*sizeof(long));
        qsort(samples, NUM_PROC*NUM_PROC, sizeof(long), longcmp);
    
        /*choose NUM_PROC - 1 pivots*/
        for (i = 0; i < NUM_PROC*NUM_PROC; i ++) {
                
        }
        for(i = 1; i < NUM_PROC; i ++) {
            sbuf[i - 1] = samples[(i)*NUM_PROC];
            printf("pivot[%d] = %ld\n", i, sbuf[i-1]);
        }
    }
    /*distribute pivots*/
    MPI_Bcast(sbuf, NUM_PROC - 1, MPI_LONG, ROOT, MPI_COMM_WORLD);

    /*Phase 3*/
    /*Partition segs according to pivots*/
    j = 0;
    part_start[0] = 0;
    for (i = 0; i < NUM_PROC - 1; i ++) {
        while(seg[j] < sbuf[i]) {
            j ++;
        }
        part_start[i + 1] = j;
        part_size[i] = j - part_start[i];
    }
    part_size[NUM_PROC - 1] = LIST_LEN/NUM_PROC - part_start[NUM_PROC - 1];

    memcpy(sbuf, seg, LIST_LEN/NUM_PROC*sizeof(long));
    
/*NOTE: I THINK WORKING UNTIL HERE*/
    /*Share location data used by MPI_Alltoallv*/
    MPI_Alltoall(part_start, 1, MPI_INT, rpart_start, 1,
                MPI_INT, MPI_COMM_WORLD);
    MPI_Alltoall(part_size, 1, MPI_INT, rpart_size, 1,
                MPI_INT, MPI_COMM_WORLD);

    /*Share all partitions*/
    MPI_Alltoallv(sbuf, part_size, part_start, MPI_LONG,
                 rbuf, rpart_size, rpart_start, MPI_LONG, MPI_COMM_WORLD);

    /*Phase 4*/
    /*Give merges to ROOT for final concatenation and result*/
    MPI_Gather(rbuf, LIST_LEN/NUM_PROC, MPI_LONG, list, LIST_LEN/NUM_PROC, 
              MPI_LONG, ROOT, MPI_COMM_WORLD);

    if (rank == ROOT) {
        printf("FINAL LIST:\n");
        for(i = 0; i < LIST_LEN; i ++) {
            printf("%d: %ld\n", i, list[i]);
        }
    }

    MPI_Finalize();
    
    return 0;
}
