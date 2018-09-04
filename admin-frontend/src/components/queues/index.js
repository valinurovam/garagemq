import React from 'react';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
import Checkbox from '@material-ui/core/Checkbox';
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
});

class Queues extends React.Component {
    constructor() {
        super();

        this.state = {
            queues: [],
        };

        this.loadQueues()
    }

    loadQueues = () => {
        AMQPAPI.get('/queues')
            .then(response => this.setState({queues: response.data.items ? response.data.items : []}))
    };

    tableItems = () => {
        const {classes} = this.props;
        let rowId = 0;

        return (
            <Table className={classes.table}>
                <TableHead>
                    <TableRow>
                        <TableCell>Virtual Host</TableCell>
                        <TableCell>Name</TableCell>
                        <TableCell>D</TableCell>
                        <TableCell>AD</TableCell>
                        <TableCell>Excl</TableCell>
                        <TableCell>Ready</TableCell>
                        <TableCell>Unacked</TableCell>
                        <TableCell>Total</TableCell>
                        <TableCell>Incoming</TableCell>
                        <TableCell>Deliver</TableCell>
                        <TableCell>Get</TableCell>
                        <TableCell>Ack</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {this.state.queues.map(row => {
                        return (
                            <TableRow key={rowId++}>
                                <TableCell>{row.vhost}</TableCell>
                                <TableCell>{row.name}</TableCell>
                                <TableCell padding={'checkbox'}><Checkbox disabled checked={row.durable}/></TableCell>
                                <TableCell padding={'checkbox'}><Checkbox disabled checked={row.auto_delete}/></TableCell>
                                <TableCell padding={'checkbox'}><Checkbox disabled checked={row.exclusive}/></TableCell>
                                <TableCell>{row.counters.ready ? row.counters.ready.value : 0}</TableCell>
                                <TableCell>{row.counters.unacked ? row.counters.unacked.value : 0}</TableCell>
                                <TableCell>{row.counters.total ? row.counters.total.value : 0}</TableCell>
                                <TableCell>{this.transformRate(row.counters.incoming)}</TableCell>
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
        setTimeout(this.loadQueues, 5000);
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

export default withStyles(styles)(Queues);