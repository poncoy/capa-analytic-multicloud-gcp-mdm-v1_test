## Initial setup steps

1. Install Google Cloud SDK for your system: https://cloud.google.com/sdk/docs/quickstart

2. Remember to run ``gcloud init`` in your terminal and select the correct user when the browser opens

3. Install Terraform for your system: https://learn.hashicorp.com/tutorials/terraform/install-cli

4. Clone the repository in your machine

5. Go to the terraform folder: ``cd`` in ``terraform/dev`` folder

6. Use your own default credentials for your application to

   access:  ``gcloud auth application-default login --scopes="https://www.googleapis.com/auth/drive.readonly","https://www.googleapis.com/auth/cloud-platform"``

   and select the correct user when the browser opens

7. Run ``terraform init -backend-config="bucket=**COMPLETE WITH THE CORRECT BUCKET NAME**"`` to download required

8. To verify if everything is running fine run ``terraform plan`` or ``terraform plan -target="google_cloudfunctions_function.aaa_test_dynamic_deploy_001"``