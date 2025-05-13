
source $CDP_DATA_CTL_HOME/env/shell/env.set.sh
source $CDP_DATA_CTL_HOME/env/shell/env.set.db.alloy.sitpub.marsol_dev_01.sh

# load taxonomies to stage
#cdp.data.ctl.sh     load    to-stage    taxonomy

# load taxonomies from stage
cdp.data.ctl.sh     load    from-stage  taxonomy

# extract from assets
#cdp.data.ctl.sh extract from-assets

