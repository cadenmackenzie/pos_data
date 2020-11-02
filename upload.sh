zip -r $1.zip .
aws lambda update-function-code --function-name $2 --zip-file fileb://$1.zip
