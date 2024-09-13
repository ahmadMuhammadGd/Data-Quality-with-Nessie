# SSHSparkOperator

## How to use?
- Create an SSH connection with hostname, password, and port
- In case of using Minio-related spark submits, add this extra field:  
```json
{
  "AWS_ACCESS_KEY_ID": "admin",
  "AWS_SECRET_ACCESS_KEY": "password",
  "AWS_REGION": "us-east-1",
  "AWS_DEFAULT_REGION": "us-east-1"
}
```
## Important Note 
DO NOT USE THIS IN PRODUCTION !!!!!!!!