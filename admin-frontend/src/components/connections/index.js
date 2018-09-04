import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import {withStyles} from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import AMQPAPI from '../../AMQPApi'
import Paper from '@material-ui/core/Paper';

const styles = theme => ({
    tableContainer: {
        width: '100%',
    },
    table: {
        width: '100%',
    },
    formContainer: {
        width: 300,
    },
    form: {
        marginLeft: theme.spacing.unit * 1,
    },
    submit: {
        marginTop: theme.spacing.unit * 3,
    },
    tableHead: {
        fontWeight: 'bold',
    }
});

class Connections extends React.Component {
    constructor() {
        super();

        this.state = {
            connections: [],
        };

        this.loadConnections()
    }

    loadConnections = () => {
        AMQPAPI.get('/connections')
            .then(response => this.setState({connections: response.data.items ? response.data.items : []}))
    };


    tableItems = () => {
        const {classes} = this.props;
        let rowId = 0;

        return (
            <Table className={classes.table}>
                <TableHead>
                    <TableRow>
                        <TableCell className={classes.tableHead}>ID</TableCell>
                        <TableCell className={classes.tableHead}>Virtual Host</TableCell>
                        <TableCell className={classes.tableHead}>Name</TableCell>
                        <TableCell className={classes.tableHead}>User name</TableCell>
                        <TableCell className={classes.tableHead}>Protocol</TableCell>
                        <TableCell className={classes.tableHead} numeric>Channels</TableCell>
                        <TableCell className={classes.tableHead}>From client</TableCell>
                        <TableCell className={classes.tableHead}>To client</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {this.state.connections.map(row => {
                        return (
                            <TableRow key={rowId++}>
                                <TableCell>{row.id}</TableCell>
                                <TableCell>{row.vhost}</TableCell>
                                <TableCell>{row.addr}</TableCell>
                                <TableCell>{row.user}</TableCell>
                                <TableCell>{row.protocol}</TableCell>
                                <TableCell numeric>{row.channels_count}</TableCell>
                                <TableCell>{this.transformTraffic(row.from_client)}</TableCell>
                                <TableCell>{this.transformTraffic(row.to_client)}</TableCell>
                            </TableRow>
                        );
                    })}
                </TableBody>
            </Table>
        )
    };

    transformTraffic = (trackValue) => {
        let value = 0;
        if (trackValue) {
            value = trackValue.value;
        }
        if (value > 1024 * 1024) {
            value = Math.round(value * 100 / 1024 / 1024) / 100;
            return value + ' MB/s';
        } else if (value > 1024) {
            value = Math.round(value * 100 / 1024) / 100;
            return value + ' KB/s';
        } else {
            return value + ' B/s';
        }
    };

    render() {
        setTimeout(this.loadConnections, 5000);
        const {classes} = this.props;

        return (
            <Grid
                container
                direction="column"
                justify="flex-start"
                alignItems="flex-start"
            >
                <Paper className={classes.tableContainer}>
                    {this.tableItems()}
                </Paper>
            </Grid>
        )
    }
}

export default withStyles(styles)(Connections);