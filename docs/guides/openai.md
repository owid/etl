---
tags:
    - 👷 Staff
---
## Setting Up OpenAI API Key

Some of our tooling require access to OpenAI's models. To use these tools, you need to set up an API key:

1. Make sure that you are added to the team on OpenAI's platform. If you are not, ask a colleague to add you.
2. Go to the [API keys](https://platform.openai.com/api-keys) page and create a new secret key.
    - Use "Restricted" permissions with: "Read" permissions for Models, Assistant and Threads, "Write" for Model Capabilities. Leave the rest with default values.
    - Copy the secret key (++cmd+v++).
3. Set the `OPENAI_API_KEY` environment variable to the secret key. We recommend setting this environment variable in your `.env` file as `OPENAI_API_KEY='sk-...'`.


If this does not work, please report it on Slack.
