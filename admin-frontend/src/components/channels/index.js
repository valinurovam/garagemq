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

class Channels extends React.Component {
    constructor() {
        super();

        this.state = {
            channels: [],
        };

        this.loadChannels()
    }

    loadChannels = () => {
        AMQPAPI.get('/channels')
            .then(response => this.setState({channels: response.data.items ? response.data.items : []}))
    };


    tableItems = () => {
        const {classes} = this.props;
        let rowId = 0;

        return (
            <Table className={classes.table}>
                <TableHead>
                    <TableRow>
                        <TableCell className={classes.tableHead}>Channel</TableCell>
                        <TableCell className={classes.tableHead}>Virtual host</TableCell>
                        <TableCell className={classes.tableHead}>User name</TableCell>
                        <TableCell className={classes.tableHead}>QOS</TableCell>
                        <TableCell className={classes.tableHead}>Unacked</TableCell>
                        <TableCell className={classes.tableHead}>Publish</TableCell>
                        <TableCell className={classes.tableHead}>Confirm</TableCell>
                        <TableCell className={classes.tableHead}>Deliver</TableCell>
                        <TableCell className={classes.tableHead}>Get</TableCell>
                        <TableCell className={classes.tableHead}>Ack</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {this.state.channels.map(row => {
                        return (
                            <TableRow key={rowId++}>
                                <TableCell>{row.channel}</TableCell>
                                <TableCell>{row.vhost}</TableCell>
                                <TableCell>{row.user}</TableCell>
                                <TableCell>{row.qos}</TableCell>
                                <TableCell>{row.counters.unacked.value}</TableCell>
                                <TableCell>{this.transformRate(row.counters.publish)}</TableCell>
                                <TableCell>{this.transformRate(row.counters.confirm)}</TableCell>
                                <TableCell>{this.transformRate(row.counters.deliver)}</TableCell>
                                <TableCell>{this.transformRate(row.counters.get)}</TableCell>
                                <TableCell>{this.transformRate(row.counters.ack)}</TableCell>
                            </TableRow>
                        );
                    })}
                </TableBody>
            </Table>
        )
    };

    transformRate = (trackValue) => {
        if (!trackValue || !trackValue.value) {
            return '0/s'
        }

        return trackValue.value + '/s'
    };

    render() {
        setTimeout(this.loadChannels, 5000);
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

export default withStyles(styles)(Channels);