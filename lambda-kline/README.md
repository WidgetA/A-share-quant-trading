# lambda-kline — overseas K-line render service

Receives OHLCV from the trading-service, renders a K-line PNG with matplotlib,
uploads it to a public-read S3 object, and returns the URL so we can hand it to
柏拉图AI's vision endpoint.

The whole hop exists to dodge mainland China ICP filing rules: matplotlib +
public storage live in `us-east-1`; the trading-service in cn-shanghai only
ships JSON over HTTPS.

## Architecture

```
trading-service (cn-shanghai)             Lambda (us-east-1)
─────────────────────────────             ────────────────────────────
1. trigger fires
2. fetch OHLCV from GreptimeDB
3. POST {code, days, ohlcv[]} ─────────► 4. matplotlib render PNG
                                          5. S3 put_object (ACL=public-read)
   { url, key, ... } ◄────────────────── 6. return public S3 URL
7. POST chat/completions to bltcy
   with {image_url: {url}}
```

## One-time AWS bootstrap

You need an AWS account with `us-east-1` as the working region. Run the
following with an admin credential, then store the names/keys in GitHub
Secrets — CI/CD takes over from there.

### 1. S3 bucket (public-read, 30-day lifecycle)

```bash
BUCKET=ashare-kline-render-$RANDOM
REGION=us-east-1

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION"

# Allow object-level public-read ACLs (off by default in modern accounts).
aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration \
  "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=true,RestrictPublicBuckets=true"

# Bucket ownership: ACLs enabled (so per-object public-read works).
aws s3api put-bucket-ownership-controls --bucket "$BUCKET" \
  --ownership-controls 'Rules=[{ObjectOwnership=BucketOwnerPreferred}]'

# Auto-delete charts older than 30 days.
cat > /tmp/lifecycle.json <<EOF
{"Rules":[{"ID":"expire-30d","Status":"Enabled","Filter":{"Prefix":"kline/"},"Expiration":{"Days":30}}]}
EOF
aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" \
  --lifecycle-configuration file:///tmp/lifecycle.json
```

### 2. ECR repository

```bash
REPO=ashare-kline-render
aws ecr create-repository --repository-name "$REPO" --region "$REGION"
# capture: <accountId>.dkr.ecr.us-east-1.amazonaws.com/ashare-kline-render
```

### 3. Lambda execution role

```bash
ROLE_NAME=ashare-kline-lambda-role

cat > /tmp/trust.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
"Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF
aws iam create-role --role-name "$ROLE_NAME" --assume-role-policy-document file:///tmp/trust.json

aws iam attach-role-policy --role-name "$ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole

cat > /tmp/s3-write.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
"Action":["s3:PutObject","s3:PutObjectAcl"],
"Resource":"arn:aws:s3:::${BUCKET}/*"}]}
EOF
aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name s3-write --policy-document file:///tmp/s3-write.json
```

### 4. First-time Lambda function (CI updates the image after this)

Build & push an initial image once (CI takes over after that):

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
IMAGE="${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com/${REPO}:latest"

aws ecr get-login-password --region "$REGION" \
  | docker login --username AWS --password-stdin "${ACCOUNT}.dkr.ecr.${REGION}.amazonaws.com"
docker build --platform linux/amd64 -t "$IMAGE" .
docker push "$IMAGE"

UPLOAD_TOKEN=$(openssl rand -hex 32)   # save this — main service needs it
ROLE_ARN=$(aws iam get-role --role-name "$ROLE_NAME" --query Role.Arn --output text)

aws lambda create-function \
  --function-name ashare-kline-render \
  --package-type Image \
  --code "ImageUri=${IMAGE}" \
  --role "$ROLE_ARN" \
  --timeout 60 \
  --memory-size 1024 \
  --architectures x86_64 \
  --environment "Variables={BUCKET_NAME=${BUCKET},UPLOAD_TOKEN=${UPLOAD_TOKEN}}"

aws lambda create-function-url-config \
  --function-name ashare-kline-render \
  --auth-type NONE \
  --cors '{"AllowOrigins":["*"],"AllowMethods":["POST","GET"]}'
# capture: FunctionUrl https://xxx.lambda-url.us-east-1.on.aws/
```

> Auth-type NONE is fine because we gate access with the `x-upload-token`
> header at the application layer.

### 5. GitHub Secrets / Variables

Repository Settings → Secrets and variables → Actions:

| Name | Type | Value |
|---|---|---|
| `AWS_ACCESS_KEY_ID` | Secret | IAM user with ECR push + lambda update permission |
| `AWS_SECRET_ACCESS_KEY` | Secret | corresponding secret key |
| `AWS_REGION` | Secret | `us-east-1` |
| `AWS_LAMBDA_KLINE_ECR` | Secret | `ashare-kline-render` (just the repo name, not the URI) |
| `AWS_LAMBDA_KLINE_FUNCTION` | Secret | `ashare-kline-render` |
| `LAMBDA_KLINE_ENABLED` | **Variable** | `true` |

Minimum IAM policy for the CI user:

```json
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":[
    "ecr:GetAuthorizationToken","ecr:BatchCheckLayerAvailability",
    "ecr:GetDownloadUrlForLayer","ecr:BatchGetImage",
    "ecr:InitiateLayerUpload","ecr:UploadLayerPart",
    "ecr:CompleteLayerUpload","ecr:PutImage"
  ],"Resource":"*"},
  {"Effect":"Allow","Action":[
    "lambda:UpdateFunctionCode","lambda:GetFunction",
    "lambda:PublishVersion"
  ],"Resource":"arn:aws:lambda:us-east-1:*:function:ashare-kline-render"}
]}
```

### 6. Trading-service configuration (Settings page, no restart needed)

The trading-service reads three values to talk to this stack. **Use the
Settings page** — it persists each value under `data/` and the orchestrator
re-reads on every request, so changes take effect immediately:

1. Open `http(s)://<trading-service>/settings` → scroll to
   **「K 线技术面分析（Lambda + 柏拉图AI）」**
2. Paste **Lambda Function URL** → 测试连接（应返回 `{"ok":true,"service":"kline-render"}`） → 保存
3. Paste **Upload Token** (same string as the Lambda's `UPLOAD_TOKEN`
   env var) → 测试 Token（应 200 / 400 = 通过；401 = token 不匹配）→ 保存
4. Paste **柏拉图AI API Key** → 验证 Key（hits `/v1/models` with the key）→ 保存

Persisted at `data/lambda_kline_url.txt`, `data/lambda_kline_token.txt`,
`data/bltcy_api_key.txt`.

**Env-var fallback** (only used when the persisted file is empty / missing —
useful for fresh container bootstrap before anyone touches the Settings page):

```
LAMBDA_KLINE_URL=https://xxx.lambda-url.us-east-1.on.aws/
LAMBDA_KLINE_TOKEN=<same UPLOAD_TOKEN as Lambda env var>
BLTCY_API_KEY=sk-xxxxxxx
# Optional:
# BLTCY_BASE_URL=https://api.bltcy.ai/v1   # only env, no UI override
# BLTCY_MODEL=gpt-5.5-pro                  # locked; emergency escape only
```

> Model is locked to `gpt-5.5-pro` —— other vision models tested were too
> shallow for technical analysis. Don't add a model selector to the UI.

## Local smoke test

```bash
curl -X POST "$LAMBDA_KLINE_URL" \
  -H "x-upload-token: $LAMBDA_KLINE_TOKEN" \
  -H "content-type: application/json" \
  -d '{"code":"000001.SZ","days":5,"ohlcv":[
    {"date":"2026-04-28","open":12.0,"high":12.3,"low":11.9,"close":12.2,"volume":1000000},
    {"date":"2026-04-29","open":12.2,"high":12.5,"low":12.1,"close":12.4,"volume":1100000},
    {"date":"2026-04-30","open":12.4,"high":12.6,"low":12.3,"close":12.45,"volume":900000},
    {"date":"2026-05-04","open":12.45,"high":12.7,"low":12.4,"close":12.6,"volume":1200000},
    {"date":"2026-05-05","open":12.6,"high":12.8,"low":12.5,"close":12.75,"volume":1050000}
  ]}'
# → {"ok":true,"url":"https://...amazonaws.com/kline/000001-SZ/...png", ...}
```

End-to-end (from trading-service):

```bash
curl -X POST http://8.133.23.9:8000/api/analyze-kline \
  -H "content-type: application/json" \
  -d '{"code":"000001.SZ","days":30}'
```

## Maintenance

- **CI auto-deploy**: any push to `main` / `refactor/cleanup-v15-only` that
  touches `lambda-kline/**` triggers a rebuild + Lambda image update.
- **Rotate UPLOAD_TOKEN**: change Lambda env var via `aws lambda
  update-function-configuration` and `LAMBDA_KLINE_TOKEN` on the
  trading-service in the same window.
- **S3 cleanup**: lifecycle rule (Step 1) auto-expires charts after 30 days.
