# AI Usage Declaration

This declaration documents the four and only four areas in which AI assistance
was used during the project. Human team members reviewed the generated work,
ran the relevant commands, and accepted the final files and results.

## 1. Complete Dataset Generation — Claude (Anthropic)

Claude assisted with the complete synthetic restaurant-dataset generation
workflow. The assistance followed the dataset requirements from planning
through validation-ready outputs.

### Workflow

1. Translate the required restaurant scenario into table entities, columns,
   identifiers, relationships, and realistic value ranges.
2. Build the generation structure for customers, locations, menus, prices,
   promotions, orders, order lines, ratings, inventory, and wastage.
3. Apply the required business patterns, date ranges, branch variation,
   transaction relationships, and controlled data-quality cases.
4. Produce raw CSV data, cleaned data, chronological splits, fixtures,
   manifests, samples, and supporting dataset metadata.
5. Run the dataset validation workflow and correct generation issues revealed
   by validation results.

### Result

The completed dataset contains the required source tables, realistic
relationships, documented quality cases, cleaned outputs, chronological
training/validation/test partitions, and Parquet-ready analytical data.

## 2. Assisted Testing and Debugging of Errors — ChatGPT

ChatGPT assisted only with testing and debugging. It was used as a diagnostic
partner after commands or tests produced errors, warnings, missing-artifact
messages, schema mismatches, or runtime failures.

### Workflow

1. Read the complete error output and identify the failing file, function,
   command, schema, or missing artifact.
2. Trace the failure to its source and compare the expected contract with the
   actual input, output, column name, path, or report structure.
3. Suggest a focused correction, replacement file, or reproducible terminal
   command.
4. Re-run the targeted test or verification command in the project environment.
5. Run the complete test suite and related pipeline checks after the targeted
   correction.

### Result

This process resolved the validation-fixture issue, restored required report
artifacts, corrected the Spark SQL channel-column contract, and confirmed the
final result with **102 passing tests and 0 failures**.

## 3. Documentation Building — Gemini

Gemini assisted with building and organizing the project documentation. The
assistance focused on explaining the completed project clearly and arranging
technical information into usable documentation sections.

### Workflow

1. Organize the project explanation into installation, architecture, dataset,
   pipeline, verification, operation, and usage sections.
2. Convert technical project information into readable explanations, tables,
   command examples, workflows, and structured reference material.
3. Align documentation language with the SRS terminology and the actual
   project folders, commands, outputs, and verification evidence.
4. Review the documentation for consistency, completeness, clear navigation,
   and reproducible instructions.
5. Update the final documentation files after the project runtime checks were
   completed.

### Result

The project documentation explains the system workflow, dataset scope,
environment setup, validation commands, database loading, dashboard startup,
API startup, roles, reports, and completed SRS status.

## 4. Blog Writing — Gamma

Gamma assisted only with preparing the project blog content. The assistance
focused on presentation and communication of the completed project for a
reader who needs a clear overview of its purpose, workflow, and outcomes.

### Workflow

1. Arrange the project story into a logical sequence: business context,
   dataset, processing workflow, analytics, dashboard, verification, and
   results.
2. Develop concise section headings, explanatory transitions, and reader-friendly
   descriptions of the project journey.
3. Present the technical workflow and achieved outcomes in an accessible form
   suitable for a project blog or public technical article.
4. Review the final draft for clarity, factual consistency, and alignment with
   the completed project evidence.

### Result

The blog material presents the DineIQ Analytics project, its restaurant
intelligence workflow, its user-facing capabilities, and its verified final
outcomes in a structured narrative format.
