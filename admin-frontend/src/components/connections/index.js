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
                        <TableCell>ID</TableCell>
                        <TableCell>Virtual Host</TableCell>
                        <TableCell>Addr</TableCell>
                        <TableCell numeric>Channels Count</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {this.state.connections.map(row => {
                        return (
                            <TableRow key={rowId++}>
                                <TableCell>{row.id}</TableCell>
                                <TableCell>{row.vhost}</TableCell>
                                <TableCell>{row.addr}</TableCell>
                                <TableCell numeric>{row.channels_count}</TableCell>
                            </TableRow>
                        );
                    })}
                </TableBody>
            </Table>
        )
    };

    render() {
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