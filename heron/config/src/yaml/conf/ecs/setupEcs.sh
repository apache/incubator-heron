#!/bin/bash

##### PLEASE NOTE USE THIS AS A REFERENCE. YOU MAY NEED TO CHANGE REGION/CLUSTER/AMI/KEYPAIR VALUES


REGIONS=""

SCOPE_AAS_PROBE_TOKEN="$1"

if [ -z "$(which aws)" ]; then
    echo "error: Cannot find AWS-CLI, please make sure it's installed"
    exit 1
fi

if [ -z "$(which ecs-cli)" ]; then
    echo "error: Cannot find AWS-CLI, please make sure it's installed"
    exit 1
fi

REGION=$(aws configure list 2> /dev/null | grep region | awk '{ print $2 }')
if [ -z "$REGION" ]; then
    echo "error: Region not set, please make sure to run 'aws configure'"
    exit 1
fi

AMI=$(aws --region $REGION ec2 describe-images  --filters Name=root-device-type,Values=ebs  Name=architecture,Values=x86_64  Name=virtualization-type,Values=hvm  Name=name,Values=*ubuntu-xenial-16.04-amd64-server-20161214  --query 'Images[*].{ID:ImageId}' --output text)

# Check that setup wasn't already run
CLUSTER_STATUS=$(aws ecs describe-clusters --clusters ecs-heron-cluster --query 'clusters[0].status' --output text)
if [ "$CLUSTER_STATUS" != "None" -a "$CLUSTER_STATUS" != "INACTIVE" ]; then
    echo "error: ECS cluster ecs-heron-cluster is active, cleanup first"
    exit 1
fi

set -euo pipefail

# Cluster
echo -n "Creating ECS cluster (ecs-heron-cluster) .. "
aws ecs create-cluster --cluster-name ecs-heron-cluster > /dev/null
echo "done"

# Security group
echo -n "Creating Security Group (ecs-heron-demo) .. "
SECURITY_GROUP_ID=$(aws ec2 create-security-group --group-name ecs-heron-securitygroup  --description 'ECS Heron' --query 'GroupId' --output text)
# Wait for the group to get associated with the VPC
sleep 5
#opening all ports as port assignment will be random
aws ec2 authorize-security-group-ingress --group-id $SECURITY_GROUP_ID --protocol tcp --port 0-65535 --cidr 0.0.0.0/0

# Key pair
echo -n "Creating Key Pair (ecs-heron-keypair, file ecs-heron-keypair.pem) .. "
aws ec2 create-key-pair --key-name ecs-heron-keypair --query 'KeyMaterial' --output text > ecs-heron-keypair.pem
chmod 600 ecs-heron-keypair.pem
echo "done"

# IAM role
echo -n "Creating IAM role (ecs-heron-role) .. "
aws iam create-role --role-name ecs-heron-role --assume-role-policy-document file://ecs-heron-role.json > /dev/null
aws iam put-role-policy --role-name ecs-heron-role --policy-name ecs-heron-policy --policy-document file://ecs-heron-policy.json
aws iam create-instance-profile --instance-profile-name ecs-heron-instance-profile > /dev/null
# Wait for the instance profile to be ready, otherwise we get an error when trying to use it
while ! aws iam get-instance-profile --instance-profile-name ecs-heron-instance-profile  2>&1 > /dev/null; do
    sleep 2
done
aws iam add-role-to-instance-profile --instance-profile-name ecs-heron-instance-profile --role-name ecs-heron-role
echo "done"


# Launch configuration
echo -n "Creating Launch Configuration (ecs-heron-launch-configuration) .. "
# Wait for the role to be ready, otherwise we get:
# A client error (ValidationError) occurred when calling the CreateLaunchConfiguration operation: You are not authorized to perform this operation.

sleep 15

TMP_USER_DATA_FILE=$(mktemp /tmp/ecs-heron-user-data-XXXX)
trap 'rm $TMP_USER_DATA_FILE' EXIT
cp set-ecs-cluster-name.sh $TMP_USER_DATA_FILE
if [ -n "$SCOPE_AAS_PROBE_TOKEN" ]; then
    echo "echo SERVICE_TOKEN=$SCOPE_AAS_PROBE_TOKEN >> /etc/ecs-heron/scope.config" >> $TMP_USER_DATA_FILE
fi

aws autoscaling create-launch-configuration --image-id $AMI --launch-configuration-name ecs-heron-launch-configuration --key-name ecs-heron-keypair --security-groups $SECURITY_GROUP_ID --instance-type t2.micro --user-data file://$TMP_USER_DATA_FILE  --iam-instance-profile ecs-heron-instance-profile --associate-public-ip-address --instance-monitoring Enabled=false
echo "done"

#set up the ecs cli config 
ecs-cli configure --force --region $REGION --access-key ecs-heron-keypair --cluster ecs-heron-cluster 

#create a CloudFormation template
ecs-cli up --force  --keypair ecs-heron-keypair --capability-iam --size 2 --instance-type m4.large

echo "Setup is ready! Please submit the ECS HERON Topology"
