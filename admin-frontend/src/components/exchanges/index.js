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

class Exchanges extends React.Component {
    constructor() {
        super();

        this.state = {
            exchanges: [],
        };

        this.loadExchanges()
    }

    loadExchanges = () => {
        AMQPAPI.get('/exchanges')
            .then(response => this.setState({exchanges: response.data.items ? response.data.items : []}))
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
                        <TableCell>Type</TableCell>
                        <TableCell>Durable</TableCell>
                        <TableCell>Internal</TableCell>
                        <TableCell>Auto Delete</TableCell>
                    </TableRow>
                </TableHead>
                <TableBody>
                    {this.state.exchanges.map(row => {
                        return (
                            <TableRow key={rowId++}>
                                <TableCell>{row.vhost}</TableCell>
                                <TableCell>{row.name}</TableCell>
                                <TableCell>{row.type}</TableCell>
                                <TableCell><Checkbox disabled checked={row.durable}/></TableCell>
                                <TableCell><Checkbox disabled checked={row.internal}/></TableCell>
                                <TableCell><Checkbox disabled checked={row.auto_delete}/></TableCell>
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

export default withStyles(styles)(Exchanges);