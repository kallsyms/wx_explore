# Comparison of the clouds

## AWS
### Pros
* Native S3

### Cons
* K8s costs ($72/mo for master)


## Azure
### Pros
* AKS master is free
* Spotinst k8s
* Premium storage is built for nearly this exact use but $$

### Cons
* smallest db is $25/mo


## GCP
### Pros
* GKE master is free
* Spotinst k8s
* tiny db tier which should be fine

### Cons


## IBM
### Pros

### Cons
* Lack of docs
* Hosted DB doesn't work (connection timeouts during any remotely intensive query); will require VM ($$)
* No spotinst


## DigitalOcean
### Pros
* No per-req charges for S3
* Good VM/k8s pricing

### Cons
* Bandwidth from droplet to S3 costs
    * could continuously shuffle instances...
* Potential rate limit issues (750rps per IP)


## Vultr
### Pros
* No per-req charges for S3
* Good VM/k8s pricing

### Cons
* Probable rate limit issues (400rps)
* Uncertain if VM<->S3 counts against bandwidth


## Linode
### Pros
* No per-req charges for S3
* Good VM pricing
* Linode<->storage free over ipv6?

### Cons
* Potential rate limit issues (750rps per IP)


## OVH
### Pros
* Pretty good pricing
* Could just do block storage (0.045/mo/GB ~= $13 for storage)


### Cons
* No hosted DB/k8s master
