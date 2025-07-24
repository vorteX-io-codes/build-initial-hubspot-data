# template-python-project
The template is a skeleton a Python project containing source and test folders. [Poetry](https://python-poetry.org/) is the dependency manager.
The development environment can be reproduced with the [Devcontainer](https://code.visualstudio.com/docs/devcontainers/containers) feature of VSCode.
The CI/CD is configured with Github Actions to launch the unit tests ([pytest](https://docs.pytest.org/en/8.2.x/)), to evaluate test coverage (pytest-cov) and to run a linter verifying coding rules ([ruff](https://docs.astral.sh/ruff/)). 

Recommended configuration:
- Windows 10/11 machine with [Windows Subsystem for Linux](https://learn.microsoft.com/fr-fr/windows/wsl/install) (WSL) feature enabled.
- VSCode editor installed on Windows and running on WSL (aka the 'local' machine in the following).
- Git installed on WSL.

## Configure the local machine
1. [Add on the local WSL machine the SSH keys](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account) to access Github repositories. We recommend to create a dedicated pair of SSH keys for the WSL machine which are different from SSH keys stored on Windows.
2. Configure Git with your user information:

```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@address"
```
1. [Share your Git credentials](https://code.visualstudio.com/remote/advancedcontainers/sharing-git-credentials) with your Devcontainer:
   - Start the SSH Agent in the background by running the following in a Bash terminal of WSL: `eval "$(ssh-agent -s)"`
   - Add your local SSH keys to the SSH agent with `ssh-add $HOME/.ssh/id_ed25519` (set the correct key file name)
   - Add these lines to your ~/.bash_profile in WSL
```bash
if [ -z "$SSH_AUTH_SOCK" ]; then
   # Check for a currently running instance of the agent
   RUNNING_AGENT="`ps -ax | grep 'ssh-agent -s' | grep -v grep | wc -l | tr -d '[:space:]'`"
   if [ "$RUNNING_AGENT" = "0" ]; then
        # Launch a new instance of the agent
        ssh-agent -s &> $HOME/.ssh/ssh-agent
   fi
   eval `cat $HOME/.ssh/ssh-agent`
   ssh-add ~/.ssh/id_ed25519 # TODO set the correct file name
fi
```
  - we recommend to restart WSL using `wsl --shutdown` in the Windows Powershell terminal with Administrator privileges.


## Create a repository from this template
1. Follow [Creating a repository from a template](https://docs.github.com/en/repositories/creating-and-managing-repositories/creating-a-repository-from-a-template) to create your own repository.
2. Clone the newly created repository on the WSL local machine.
3. `poetry install` create a virtual environment and install the Python packages
4. `poetry shell` activate the virtual environment in the terminal 

## Configure your project
The project uses [Poetry](https://python-poetry.org/) as dependency manager.
1. Rename the `project_name` folder containing the source code.
2. In the file `pyproject.toml`, update the values having the `TODO` comments to match project name

The project relies on Github Actions for continuous integration. In the file `.github/workflows/python-test.yml`, update the values having the `TODO` comments to match project name. Continuous integration tests the python code, evaluate code coverage and check the code with a linter.


## Reproduce the development environment
To reproduce the development environment in the VSCode editor, the repository contains a [Devcontainer](https://code.visualstudio.com/docs/devcontainers/containers) configuration file.
This allows you to start a Docker container with Python already installed along with Poetry for dependency management.

1. Open the folder containing the repository with VSCode.
2. Set in the `devcontainer.json` file the Python version of the Docker image you want to use for development.
3. Use the VSCode command 'Reopen in container' and test you can contact Github with `git fetch`.
4. You are ready to develop now !

## Linting, unit tests and test coverage

Run the linter from your local machine to verify coding rules:

```bash
ruff check 
```

Tests are defined in the `tests` folder in this project. Run unit tests and measure test coverage from your local machine using:


```bash
pytest --cov=project_name --cov-report term-missing tests/ # set the project name
```



