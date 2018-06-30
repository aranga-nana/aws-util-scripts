#!/bin/bash
export LAMBDA_HANDLER=rds_aurora_stopinator.lambda_handler
lambda-deploy --name rds-aurora-stopinator -r arn:aws:iam::725126218667:role/stopinator-role  deploy
