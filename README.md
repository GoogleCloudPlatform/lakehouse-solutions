*Copyright 2026 Google LLC*

*Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at*

*http://www.apache.org/licenses/LICENSE-2.0*

*Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*

# Google Cloud Lakehouse Solutions

Welcome to the **Google Cloud Lakehouse Solution** repository! 

## Overview
This is a repository of Lakehouse (horizontal) solutions powered by Google Cloud products with robust open-source support. 

## Motivation
The primary motivation of this repository is to demystify the lakehouse stack on Google Cloud. By providing simple, concrete, deployable sample solutions, reference architecture, design patterns, and best practices for building lakehouses on Google Cloud using an open ecosystem we hope to simplify your lakehouse adoption journey on Google Cloud.

<hr>

## Disclaimer
1. This repository and its contents are not an official Google Product.
2. The code in this repository ***is intended for educational purposes***. While it can be used to quickstart development by enterprises, due diligence and hardening of the code is required for deployment to higher environments.
3. The architecture showcased ***does not dictate*** architecture, but merely how to use Google Cloud products to solve business problems.

<hr>

## Format & Duration
The solutions are fully scripted (no research needed), with data, code, commands, orchestration, and configuration provided.

**Option 1 - DIY:** <br> 
Clone the repo and follow the step-by-step instructions for an end-to-end developer experience.

**Option 2 - Instructor-led workshop:** <br>
If you are a Google Cloud customer, you can reach out to your Google account team, and ask for an (no-cost) instructor-led workshop running on your GCP project (you will be responsible for GCP consumption).

**Option 3 - Strapped for time? Read like a book:** <br>
The repo is rich with visuals and documentation, and can be read like a book.

**Time commitment for each option:** <br>
The time commitment depends on the option chosen, solution chosen, and is documented in the solution's README.md file.

<hr>

## Dependencies
1. A Google Cloud project
2. IAM permissions to provision services, and grant IAM permissions
3. Basic knowledge of Google Cloud is useful, as is knowledge of comparable platforms and technical stacks, but not required as comprehensive instructions are included
4. If a feature showcased is not accessible by you, it is likely a private preview feature and needs explicit allow-listing by the responsible product team. Reach out to your Google Cloud account team for help with allow-listing.

<hr>

## Level
L200 - L300

<hr>

## Solutions
| # | Solution | Description | Technical Stack |
| :-- | :--- | :--- | :--- |
| 1. | [Data engineering at scale with Apache Spark](./solutions/spark-serverless-quickstart/lab-manuals/ts1-manual.md) | A quickstart guide to running Spark batch jobs  | PySpark on Managed Spark Serverless, BigQuery, Cloud Storage, Terraform |
| 2. | [Spark notebooks for data analysis, and data science](./solutions/spark-serverless-quickstart/lab-manuals/ts2-manual.md) | A quickstart guide to running Spark notebooks | Same infra as above, Spark in Colab notebooks, Spark on Jupyter on Gemini Agent Platform Workbench instance, Spark on BYO Jupyter notebook, Spark notebooks in IDEs with Data Agent Kit plugin |

<hr>

## Issues
Share your feedback, and issues encountered, by logging issues.

<hr>

## Cleanup
If you provisioned a GCP project to run through this content, you can simply shut down the project to stop spend. Alternately, you can delete instances, shut down individual services.

<hr>

## Contributing
Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING](CONTRIBUTING.md) for more information on how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms. See [Code of Conduct](CODE_OF_CONDUCT.md) for more information.

<hr>

## Authors
This repository and content is maintained by the Google Agentic Data Cloud Solution Architect team.

| # | Contributor  | Role |
| -- | :-- |  :--- |
| 1. | Anagha Khanolkar | Vision, author & technical architect |


<hr>

## Credits
Special thanks to all the contributors, partners, and the open-source community who have supported and provided feedback on these reference architectures.

| # | Name | Role | Contribution type |
| -- | :-- |  :--- | :--- |
|  |  |  | |

<hr>

## License
Apache 2.0 - See [LICENSE](LICENSE) for more information.

<hr>
