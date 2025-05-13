
# never use aliases in scripts.  won't work.  use functions instead.

# general aliases
echo ">> aliases - general"
alias   myp="""
            cd ~/MyFiles/MyProjects                         """
alias   h=history
alias   a=alias
alias   d=dirs
alias   hn=hostname
alias   p=pwd
alias   l='ls -l'
alias   la='ls -la'
alias   lt='ls -lt'
alias   h=history
alias   fs='typeset -F'
alias   vncstart='vncserver -geometry 1580x1000'
alias   eon='set -v'
alias   eoff='set +v'
alias   myf='cd ~/MyFiles'
alias   myp='cd ~/MyFiles/MyProjects'
alias   mypf='cd ~/MyFiles/MyProgramFiles'

alias   infra.alias='alias | grep infra_ | sort'

alias   git.config.set.pxmitchell="""
            source $HELPER_ENV_HOME_ENV/Git/env.set.github.pxmitchell.sh            """
alias   git.config.set.pxmtech="""
            source $HELPER_ENV_HOME_ENV/Git/env.set.github.pxmtech.sh            """
alias   git.config.set.paul-mitchell-sitpub="""
                source $HELPER_ENV_HOME_ENV/Git/env.set.github.paul-mitchell-sitpub.sh            """
alias   git.config.set.paul-mitchell-omnidatalabs="""
                source $HELPER_ENV_HOME_ENV/Git/env.set.github.paul-mitchell-omnidatalabs.sh            """

echo ">> aliases - home directories"
alias   helper-env.home="""
            cd $INFRA_HOME                                  """
alias   helper-env.home.bin="""
            cd $INFRA_HOME_BIN'                             """
alias   helper-env.home.api="""
            cd $INFRA_HOME_API                              """
alias   helper-env.home.db="""
            cd $INFRA_HOME_DB                               """
alias   helper-env.home.out="""
            cd $INFRA_HOME_OUT                              """

echo ">> aliases - environment operations"
alias   helper-env.env.set="""
            source $INFRA_HOME/Env/env.set.sh               """
alias   helper-env.env.get="""
            source $INFRA_HOME/Env/env.get.sh               """
alias   helper-env.env.get.path="""
            python $INFRA_HOME_BIN/infra.path.get.py        """
alias   helper-env.env.reset.pythonpath="""
        """

echo ">> aliases - set git variables environment variables"
alias   helper-env.git.config.set.pxmitchell="""
            source $INFRA_HOME/Env/Git/env.set.github.pxmitchell.sh            """
alias   helper-env.git.config.set.pxmtech="""
            source $INFRA_HOME/Env/Git/env.set.github.pxmtech.sh            """
alias   helper-env.git.config.set.paul-mitchell-sitpub="""
            source $INFRA_HOME/Env/Git/env.set.github.paul-mitchell-sitpub.sh            """
alias   helper-env.git.config.set.paul-mitchell-omnidatalabs="""
            source $INFRA_HOME/Env/Git/env.set.github.paul-mitchell-omnidatalabs.sh            """

#echo ">> aliases - help"
#alias   infra.help="""
#            env     | sort | grep -i infra_;
#            alias   | sort | grep -i infra.                  """