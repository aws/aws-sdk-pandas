#!/usr/bin/env bash
set -e

./setup.sh

code --install-extension DavidAnson.vscode-markdownlint
code --install-extension VisualStudioExptTeam.vscodeintellicode
code --install-extension be5invis.toml
code --install-extension donjayamanne.githistory
code --install-extension ms-azuretools.vscode-docker
code --install-extension ms-python.python
code --install-extension ms-python.vscode-pylance
code --install-extension redhat.vscode-yaml
code --install-extension twixes.pypi-assistant
code --install-extension aws-scripting-guy.cform

python_path=$(which python)

vscode_settings='{
  "explorer.autoReveal": false,
  "files.autoSave": "onFocusChange",
  "files.exclude": {
    "**/*.egg-info": true,
    "**/.mypy_cache/": true,
    "**/.pytest_cache/": true,
    "**/__pycache__/": true
  },
  "outline.showArrays": false,
  "outline.showConstants": false,
  "outline.showFields": false,
  "outline.showKeys": false,
  "outline.showNull": false,
  "outline.showStrings": false,
  "outline.showTypeParameters": false,
  "outline.showVariables": false,
  "python.analysis.extraPaths": [
  ],
  "python.analysis.memory.keepLibraryAst": true,
  "python.formatting.blackArgs": ["--line-length 120", "--target-version py36"],
  "python.formatting.provider": "black",
  "python.languageServer": "Pylance",
  "python.linting.enabled": true,
  "python.linting.flake8Args": ["--max-line-length 120"],
  "python.linting.flake8Enabled": true,
  "python.linting.mypyCategorySeverity.error": "Hint",
  "python.linting.mypyCategorySeverity.note": "Hint",
  "python.linting.mypyEnabled": true,
  "python.linting.pylintEnabled": false,
  "python.pythonPath": "'$python_path'",
  "python.terminal.activateEnvironment": true,
  "workbench.editor.labelFormat": "default",
  "workbench.tree.indent": 16,
  "yaml.customTags": [
    "!Equals sequence",
    "!FindInMap sequence",
    "!GetAtt",
    "!GetAZs",
    "!ImportValue",
    "!Join sequence",
    "!Ref",
    "!Select sequence",
    "!Split sequence",
    "!Sub"
  ]
}
'

rm -rf .vscode
mkdir .vscode
echo "$vscode_settings" > .vscode/settings.json
