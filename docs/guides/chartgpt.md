
## Using ChartGPT

!!! warning "This is an experimental tool. Please check with the team if you have questions."

!!! warning "OpenAI credentials"
    To run the following steps, add your [OpenAI API key](https://platform.openai.com/account/api-keys) to your `.env` file. E.g. `OPENAI_API_KEY=sk-...`.

We have an experimental tool that uses chatGPT to review our titles and subtitles in our chart revisions. Follow these steps to run it:

- You need to have submitted chart revisions to admin.
- Then, run
    ```
    poetry run etl chart-gpt
    ```
    You can check all the available options with `--help`.
- This generates new subtitles and titles of the chart revisions and pushes these to the `suggested_chart_revisions` table. It stores these in the column `experimental` of this same table.
- Once this is done running, you can visualise these new subtitles/titles from the admin site. You will see a light blue button showing the model name (`gpt-4` or `gpt-3.5-tubo`). Click it to render the config produced by chatGPT.
- If you approve while a gpt-based FASTT is rendered, this will be submitted.

!!! note
    - If someone already ran the command, and some charts already have gpt-based titles/subtitles, you can force overwrite these by using the option `--overwrite`.
    - By default, `gpt-3.5-turbo` is used. Change this with the option `model-name`.
    - We use a custom system prompt, but you can use another one by using the option `--system-prompt`.


### Example calls of `etl chart-gpt`

#### Only generate chatGPT revisions for me
Only create these for revisions created by you:
```
poetry run etl chart-gpt -me
```

#### Force overwrite new chatGPT revisions

```
poetry run etl chart-gpt -f
```

#### Other options
Generate revision for chart revision with id `123` using model `gpt-4` and a custom system prompt stored in `custom-system-prompt.txt` file.

```
poetry run etl chart-gpt -i 123 -n "gpt-4" -t custom-system-prompt.txt
```
