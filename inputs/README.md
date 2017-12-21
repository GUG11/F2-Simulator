# Configuration

To faciliate configuration management and parameter tuning, F2-Sim uses two json config files, one for dag specification and the other for cluster specification.

## Cluster

* time_step: schedule every time_step, double
* global_partitions_per_machine: number of global partitions per machine, int
* max_partitions_in_task: max number of partitions one task can handle, int
* machines: array of maps
    * disk: disk size for intermediate output, double
    * resources: resources, array
    * failure_rate: double,
    * replica: number of machines, int
* policies: map
    * interjob: inter job resource share, string {'FAIR', 'DRF' (Domain Resource Fairness)}
    * intrajob: intra job task scheduler, string {'CP' (Critical Path)}
    * data: data deployment in DS, string {'LQU' (Least quota usage), 'PCS' (Parent-Child Separation)}

## Dag

