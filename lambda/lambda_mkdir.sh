#!/bin/bash

# AWS CLI를 사용하여 모든 Lambda 함수의 이름을 가져옴
function_names=$(aws lambda list-functions --query 'Functions[*].[FunctionName]' --output text)

# 각 함수 이름에 대한 디렉토리를 생성하고, 해당 디렉토리에 $function_name.py 파일을 생성
for function_name in $function_names
do
    mkdir -p $function_name
    touch "${function_name}/${function_name}.py"
done

