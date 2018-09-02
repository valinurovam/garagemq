import React from 'react';
import PropTypes from 'prop-types';
import {Link, Route, Switch, withRouter} from 'react-router-dom'
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Typography from '@material-ui/core/Typography';
import {withStyles} from "@material-ui/core/styles/index";
import classNames from 'classnames';
import CssBaseline from '@material-ui/core/CssBaseline';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import Divider from '@material-ui/core/Divider';

import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import DashboardIcon from '@material-ui/icons/Dashboard';
import Cast from '@material-ui/icons/Cast';
import DeviceHub from '@material-ui/icons/DeviceHub';
import PowerInput from '@material-ui/icons/PowerInput';
import CloudQueue from '@material-ui/icons/CloudQueue';


import Exchanges from './components/exchanges';
import Connections from './components/connections';
import Queues from './components/queues';
import Overview from './components/overview';

const drawerWidth = 200;

let menuItems = [
    {
        route: '/', text: 'Overview', icon: () => {
            return (<DashboardIcon/>)
        }
    },
    {
        route: '/connections', text: 'Connections', icon: () => {
            return (<Cast/>)
        }
    },
    {
        route: '/channels', text: 'Channels', icon: () => {
            return (<PowerInput/>)
        }
    },
    {
        route: '/exchanges', text: 'Exchanges', icon: () => {
            return (<DeviceHub/>)
        }
    },
    {
        route: '/queues', text: 'Queues', icon: () => {
            return (<CloudQueue/>)
        }
    },
];

const styles = theme => ({
    root: {
        display: 'flex',
    },
    toolbar: {
        paddingRight: 24, // keep right padding when drawer closed
    },
    toolbarIcon: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'flex-end',
        padding: '0 8px',
        ...theme.mixins.toolbar,
    },
    appBar: {
        zIndex: theme.zIndex.drawer + 1,
        transition: theme.transitions.create(['width', 'margin'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.leavingScreen,
        }),
    },
    appBarShift: {
        marginLeft: drawerWidth,
        width: `calc(100% - ${drawerWidth}px)`,
        transition: theme.transitions.create(['width', 'margin'], {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
        }),
    },
    title: {
        flexGrow: 1,
    },
    drawerPaper: {
        position: 'relative',
        whiteSpace: 'nowrap',
        width: drawerWidth,
        transition: theme.transitions.create('width', {
            easing: theme.transitions.easing.sharp,
            duration: theme.transitions.duration.enteringScreen,
        }),
    },
    appBarSpacer: theme.mixins.toolbar,
    content: {
        flexGrow: 1,
        padding: theme.spacing.unit * 3,
        height: '100vh',
        overflow: 'auto',
    },
    chartContainer: {
        marginLeft: -22,
    },
    tableContainer: {
        height: 320,
    },
});

class App extends React.Component {
    menuItems = () => {
        let result = [];
        for (let i = 0; i < menuItems.length; i++) {
            let item = menuItems[i];
            result.push(
                <ListItem button key={i} component={Link} to={item.route}>
                    <ListItemIcon>
                        {item.icon()}
                    </ListItemIcon>
                    <ListItemText primary={item.text}/>
                </ListItem>);
        }

        return result;
    };

    currentPage = () => {
        const {location} = this.props;
        let item = menuItems.find(e => e.route === location.pathname);
        if (!item) {
            return 'Not Found'
        }

        return item.text;
    };

    render() {
        const {classes} = this.props;

        return (
            <React.Fragment>
                <CssBaseline/>
                <div className={classes.root}>
                    <AppBar
                        position="absolute"
                        className={classNames(classes.appBar, classes.appBarShift)}
                    >
                        <Toolbar disableGutters={false} className={classes.toolbar}>
                            <Typography variant="title" color="inherit" noWrap className={classes.title}>
                                {this.currentPage()}
                            </Typography>
                            <Typography variant="title" color="inherit" className={classes.flex}>
                                GarageMQ
                            </Typography>
                        </Toolbar>
                    </AppBar>
                    <Drawer
                        variant="permanent"
                        classes={{
                            paper: classNames(classes.drawerPaper),
                        }}
                        open={true}
                    >
                        <div className={classes.toolbarIcon}>

                        </div>
                        <Divider/>
                        <List>{this.menuItems()}</List>
                        <Divider/>
                    </Drawer>
                    <main className={classes.content}>
                        <div className={classes.appBarSpacer}/>
                        <Switch>
                            <Route exact path='/' component={Overview}/>
                            <Route exact path='/exchanges' component={Exchanges}/>
                            <Route exact path='/connections' component={Connections}/>
                            <Route exact path='/queues' component={Queues}/>
                        </Switch>

                    </main>
                </div>
            </React.Fragment>
        )
    }
}

App.propTypes = {
    classes: PropTypes.object.isRequired,
    selected: PropTypes.string,
};


export default withStyles(styles)(withRouter(App));