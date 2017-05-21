#!/bin/bash

aws autoscaling delete-launch-configuration --launch-configuration-name ecs-heron-launch-configuration
echo "done delete-launch-configuration"
aws iam remove-role-from-instance-profile --instance-profile-name ecs-heron-instance-profile --role-name ecs-heron-role
echo "done iam remove-role-from-instance-profile"
aws iam delete-instance-profile --instance-profile-name ecs-heron-instance-profile
echo "done iam iam delete-instance-profile"
aws iam delete-role-policy --role-name ecs-heron-role --policy-name ecs-heron-policy
echo "done iam delete-role-policy "
aws iam delete-role --role-name ecs-heron-role
echo "done iam delete-role "
aws ec2 delete-key-pair --key-name ecs-heron-keypair
rm -f ecs-heron-key*.pem

GROUP_ID=$(aws ec2 describe-security-groups --query 'SecurityGroups[?GroupName==`ecs-heron-securitygroup`].GroupId' --output text)
aws ec2 delete-security-group --group-id "$GROUP_ID"
echo "done delete security-groups "

aws ecs delete-cluster --cluster ecs-heron-cluster
echo "done delete cluster "
