# Deploying Heron

Heron is designed to be run in clustered, scheduler-driven environments. It
currently supports three scheduler options out of the box:

* [Aurora](schedulers/aurora)
* [Mesos](schedulers/mesos)
* [Local scheduler](schedulers/local)

To implement a new scheduler, see
[Implementing a Custom Scheduler](../../contributors/custom-scheduler).
