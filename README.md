```bash
python main.py \
    --aws-access-key-id ACCESS_KEY_ID \
    --aws-secret-access-key SECRET_ACCESS_KEY \
    --aws-region REGION \
    --aws-cloudwatch-group LOGGROUP \
    --aws-cloudwatch-stream LOG_STREAM \
    --docker-image python \
    --bash-command "python -c \"print('Hello, World!')\""
```
