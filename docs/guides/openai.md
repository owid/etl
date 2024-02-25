
## Setting Up OpenAI API Key

The `etl metadata-upgrade` tool requires the `OPENAI_API_KEY` environment variable to function properly. This is the API key provided by OpenAI for accessing their GPT model.

To obtain the API key, you need to sign up on the [OpenAI platform](https://openai.com). After signing up, you can find your API key in the account [settings](https://platform.openai.com/api-keys). For more information see OpenAI [FAQs](https://help.openai.com/en/articles/4936850-where-do-i-find-my-api-key)

Once you have the API key, set it as an environment variable in your development environment. For example, in a Unix-based system, you can add the following line to your `.bashrc` or `.zshrc`:

```bash
export OPENAI_API_KEY='your-api-key'
```
