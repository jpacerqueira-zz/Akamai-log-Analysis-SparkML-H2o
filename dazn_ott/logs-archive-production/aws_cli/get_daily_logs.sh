#!/bin/bash
#
##
#
DATE_V1=$1
if [ -z "$DATE_V1" ]
then
 DATE_V1=190101
fi
#
echo $DATE_V1
# Make sure security Kerberos is cached
#
if [ -f ~/.keytabs/$(whoami).keytab ]; then
  # for any:any credentials
  export KRB5CCNAME=/tmp/krb5cc_$(id -u)

  kinit -kt ~/.keytabs/$(whoami).keytab $(whoami)/chpbda@BDA2.PERFORMGROUP.COM -c /tmp/krb5cc_$(id -u)
  klist /tmp/krb5cc_$(id -u)
else
  echo "No Kerberos here"
fi
#
#   GENERAL ACTION STARTS HERE
#
MYFOLDER=/home/siemanalyst/projects/logs-archive-production/aws_cli
#
mkdir -p ${MYFOLDER}/data/raw/${DATE_V1}
#
#/opt/cloudera/parcels/Anaconda/bin/python3.6 ~/.local/bin/aws --version
#
/opt/cloudera/parcels/Anaconda/bin/python3.6 ~/.local/bin/aws s3 ls s3://logs-archive-production/${DATE_V1}/ 
#
#
rm -rf $PWD/data/raw/${DATE_V1}/*.*
/opt/cloudera/parcels/Anaconda/bin/python3.6 ~/.local/bin/aws s3 cp s3://logs-archive-production/${DATE_V1}/  ${MYFOLDER}/data/raw/${DATE_V1}/ --recursive --exclude "*"  --include  "*.log.gz"
#
hdfs dfs -mkdir -p hdfs:///data/raw/ott_dazn/logs-archive-production/dt=20${DATE_V1}
hdfs dfs -rm -f -skipTrash hdfs:///data/raw/ott_dazn/logs-archive-production/dt=20${DATE_V1}/*.*
hdfs dfs -copyFromLocal ${MYFOLDER}/data/raw/${DATE_V1}/*.* hdfs:///data/raw/ott_dazn/logs-archive-production/dt=20${DATE_V1}
rm -rf ${MYFOLDER}/data/raw/${DATE_V1}/*.*
#
exit 0
#
