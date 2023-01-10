# protohacker-py

Protohacker challenges implementation in Python


## Fly.io

In order to deploy to fly.io follow the steps below:

```
# Authenticate to fly.io
$ flyctl auth login

# use the existing fly.toml
$ flyctl launch --name protohacker-py

# this command uses the local docker instance to build the image).
$ flyctl deploy --local-only
```
## Other implementations:
- Golang: https://github.com/snobb/protohacker
- Typescript: https://github.com/snobb/protohacker-ts

## Links
- Protohacker - https://protohackers.com/
- Discord - [Protohacker chat server](https://discord.gg/RqqmGePnWU)

## Dev environment

```
$ python3.11 -m venv .pyenv
$ source .pyenv/bin/activate
(.pyenv) $ pip install mypy flake8 pyright yapf
```
