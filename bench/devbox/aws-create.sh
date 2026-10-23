#!/usr/bin/env bash
# Creates t4's dev box on AWS: a Spot i7i.48xlarge in us-east-1, 192
# vCPUs, 1.5 TiB, and twelve 3.75 TB instance-store SSDs at about 4.27 GB/s
# read each (ScyllaDB's measurements, scylla-machine-image's
# aws_io_params.yaml), past what GCP lets the project run.
#
# The account is shared, so the box brings its own key pair and security
# group, both named t4-bench-$USER, the group admitting SSH from this
# machine's address only. It is a one-time Spot instance that terminates on
# shutdown: startup.sh, its user data, mounts the SSDs at /mnt/t4/<n> and
# powers it off once idle for 30 minutes, and then nothing of it is left.
#
#   AWS_PROFILE=spiral bench/devbox/aws-create.sh
#   ssh -i ~/.ssh/t4-bench ubuntu@<address it prints>
set -euo pipefail

REGION="${REGION:-us-east-1}"
ZONE="${ZONE:-us-east-1d}"
INSTANCE_TYPE="${INSTANCE_TYPE:-i7i.48xlarge}"
NAME="t4-bench-$USER"
KEY=~/.ssh/t4-bench
aws=(aws --region "$REGION")

[ -e "$KEY" ] || ssh-keygen -q -t ed25519 -N '' -C "$NAME" -f "$KEY"
"${aws[@]}" ec2 describe-key-pairs --key-names "$NAME" >/dev/null 2>&1 ||
    "${aws[@]}" ec2 import-key-pair --key-name "$NAME" \
        --public-key-material "fileb://$KEY.pub" >/dev/null

vpc=$("${aws[@]}" ec2 describe-vpcs --filters Name=is-default,Values=true \
    --query 'Vpcs[0].VpcId' --output text)
group=$("${aws[@]}" ec2 describe-security-groups \
    --filters Name=vpc-id,Values="$vpc" Name=group-name,Values="$NAME" \
    --query 'SecurityGroups[0].GroupId' --output text)
if [ "$group" = None ]; then
    group=$("${aws[@]}" ec2 create-security-group --group-name "$NAME" --vpc-id "$vpc" \
        --description "SSH to the t4 dev box of $USER" --query GroupId --output text)
fi
address=$(curl -s https://checkip.amazonaws.com)
"${aws[@]}" ec2 authorize-security-group-ingress --group-id "$group" \
    --protocol tcp --port 22 --cidr "$address/32" >/dev/null 2>&1 || true

subnet=$("${aws[@]}" ec2 describe-subnets \
    --filters Name=vpc-id,Values="$vpc" Name=availability-zone,Values="$ZONE" \
    Name=default-for-az,Values=true --query 'Subnets[0].SubnetId' --output text)
image=$("${aws[@]}" ssm get-parameter \
    --name /aws/service/canonical/ubuntu/server/26.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query Parameter.Value --output text)

instance=$("${aws[@]}" ec2 run-instances \
    --instance-type "$INSTANCE_TYPE" \
    --image-id "$image" \
    --subnet-id "$subnet" \
    --security-group-ids "$group" \
    --key-name "$NAME" \
    --instance-market-options 'MarketType=spot,SpotOptions={SpotInstanceType=one-time,InstanceInterruptionBehavior=terminate}' \
    --instance-initiated-shutdown-behavior terminate \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=150,VolumeType=gp3,DeleteOnTermination=true}' \
    --user-data "file://$(dirname "$0")/startup.sh" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME},{Key=owner,Value=$USER},{Key=purpose,Value=t4-bench}]" \
        "ResourceType=volume,Tags=[{Key=Name,Value=$NAME},{Key=owner,Value=$USER},{Key=purpose,Value=t4-bench}]" \
    --query 'Instances[0].InstanceId' --output text)
"${aws[@]}" ec2 wait instance-running --instance-ids "$instance"
"${aws[@]}" ec2 describe-instances --instance-ids "$instance" \
    --query 'Reservations[0].Instances[0].[InstanceId,PublicIpAddress]' --output text
