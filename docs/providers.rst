Provider-Specific Documentation
================================

Although Cloud Tasks attempts to be provider-independent, there are nevertheless some
provider-specific features and issues that are exposed to the user. We try to cover these
here.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   provider_aws
   provider_gcp


Environment Variables
---------------------

Cloud Tasks can use environment variables for credentials:

- AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- GCP: GOOGLE_APPLICATION_CREDENTIALS
- Azure: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
