

PROJECT_ID=$(gcloud config list project --format "value(core.project)")
BUCKET_NAME=${PROJECT_ID}-credit-default
echo $BUCKET_NAME
REGION=us-central1


gsutil mb -l $REGION gs://$BUCKET_NAME

gsutil cp /home/yourlocation/model.joblib  gs://credit-default-credit-default/


###create model name

gcloud ml-engine models create "version1"

### model version

MODEL_DIR="gs://credit-default-credit-default/"
VERSION_NAME="version_1"
MODEL_NAME="version1"
FRAMEWORK="SCIKIT_LEARN"

gcloud ai-platform versions create $VERSION_NAME \
--model $MODEL_NAME \
--origin $MODEL_DIR \
--runtime-version=1.13 \
--framework $FRAMEWORK \
--python-version=3.5


gcloud ml-engine versions describe $VERSION_NAME \
--model $MODEL_NAME
