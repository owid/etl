---
tags:
  - 👷 Staff
status: new
icon: lucide/sparkles
---

# AI-powered workflow

!!! tip "Let's jump right in!"

    Start Claude from your terminal with command
    ```
    claude
    ```

    Ask it to explain to you what it can assist you with:

    ```
    > What ETL tasks can you help me with?
    ```


[:octicons-link-external-16: Claude Code](https://www.claude.com/product/claude-code) is an AI-powered CLI tool that can help you with various ETL development tasks: from understanding complex code patterns to implementing new datasets, debugging data quality issues, and maintaining our codebase.

At the moment it is mostly useful for updating datasets.

!!! warning "Work in Progress"

    This AI workflow is actively evolving! Updating a dataset was a proof of concept, and we're continuously improving Claude's capabilities and documentation based on real-world usage.

    **Have ideas or feedback?** Your input is highly appreciated! If you discover new useful patterns or encounter issues, please:

    - Share your experience in #data-scientists on Slack
    - Suggest improvements to CLAUDE.md or this documentation
    - Help us document new use cases as we discover them

## Try it!
If you are wondering what Claude is capable of doing, just ask it. Claude can explain its own capabilities and help you understand how to use it effectively:

```
> What ETL tasks can you assist me with?
```

Or, in general, you can ask Claude for help on how to use it:
```
> How do I [task]?
```

### Updating a dataset
A typical, and well supported, task includes updating an existing dataset. Claude is able to create a pull request, create snapshot and data step templates, run and fix them, update indicators, etc.

```
> Help me create a new garden step for WHO tuberculosis data. It is under namespace 'who', short name 'tuberculosis'.
```

### Other examples


> Explain how the PathFinder pattern works in our ETL steps


> What does the geo.harmonize_countries() function do?

## Best Practices

<div class="annotate" markdown>

:material-check-circle:{ style="color: green" } **Ask naturally** - Claude understands conversational requests

:material-check-circle:{ style="color: green" } **Be specific about context** - Mention dataset names, namespaces, versions

:material-check-circle:{ style="color: green" } **Request validation** - Ask Claude to run tests after changes

:material-check-circle:{ style="color: green" } **Check generated code** - Review AI-generated code before committing

:material-check-circle:{ style="color: green" } **Use iterative refinement** - Iterate on solutions if the first attempt isn't perfect

:material-close-circle:{ style="color: red" } **Don't bypass project rules** - Claude follows OWID conventions (e.g., always uses `.venv/bin/`)

:material-close-circle:{ style="color: red" } **Don't commit without review** - Always review changes before pushing

:material-close-circle:{ style="color: red" } **Don't mask problems** - Ask Claude to trace issues systematically, not create workarounds

:material-close-circle:{ style="color: red" } **Don't skip `make check`** - Let Claude run it, or run it yourself before committing

</div>


!!! warning "Always double-check AI-generated code"

    While Claude is trained on best practices and understands our conventions, always:

    - Review generated code for correctness
    - Run `make check` before committing
    - Test ETL steps with `--dry-run` first
    - Verify database queries on staging before production


## How does Claude know?

It has access to your workspace, along with various documentation files in the repository, including:

- [`CLAUDE.md`](https://github.com/owid/etl/tree/master/CLAUDE.md)
- [`.claude/commands`](https://github.com/owid/etl/tree/master/.claude/commands)
- [`.claude/agents`](https://github.com/owid/etl/tree/master/.claude/agents)
- etc.

Have a look at these files to better understand its references.
