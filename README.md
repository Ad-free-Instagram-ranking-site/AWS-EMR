# AWS-EMR
WebPage For Best Instagram restaurants.

Language : Pyspark, Sparksql

---Make EMR---
IAM authority 
plus below authority
For EMRDescribeCluster


"Statement": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:DescribeCluster",
            "Resource": "arn:aws:elasticmapreduce:ap-northeast-2:981886163709:cluster/*"
For EMR


"Statement": [
        {
            "Effect": "Allow",
            "Action": "elasticmapreduce:*",
            "Resource": "*"
        }
    ]
FOR S3


"Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }
    ]

    
With this EMR authority you can make proper EMR cluster that has all authority

Change Inbound rule in the Primary Node
TCP - All, Now ip 

Enter in the Zeppelin Notebook -> set the interpreter to spark
Type code

