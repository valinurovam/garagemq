import React from 'react';
import {withStyles} from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Button from '@material-ui/core/Button';
import Typography from '@material-ui/core/Typography';
import {CartesianGrid, Legend, Line, LineChart, XAxis, YAxis} from 'recharts'
import moment from 'moment'
import AMQPAPI from '../../AMQPApi'
import {Link} from "react-router-dom";

const styles = theme => ({
    colorKey: {
        float: 'left',
        width: '10px',
        height: '10px',
        margin: '3px 5px 0 0',
    },
    legendItem: {
        float: 'left',
        width: '200px',
        marginBottom: '3px',
    },
    chartTitle: {
        textAlign: 'center',
        marginBottom: '10px',
    },
    chartsContainer: {
        marginTop: '30px',
    },
    chartDiv: {
        marginBottom: '20px',
        borderLeft: '1px solid rgba(0, 0, 0, 0.23)',
    },
    button: {
        marginRight: '20px',
    },
    buttonCount: {
        marginLeft: '5px',
    },
});

const metricLabels = {
    'server.ready': "Ready",
    'server.unacked': "Unacked",
    'server.total': "Total",

    'server.publish': "Publish",
    'server.deliver': "Deliver",
    'server.acknowledge': "Acks",
    'server.get': "Get",
    'server.confirm': "Confirm",
    'server.traffic_in': "Traffic In",
    'server.traffic_out': "Traffic Out",
};

class Overview extends React.Component {
    constructor() {
        super();

        this.state = {
            metrics: {},
            counters: {},
        };

        this.loadMetrics();
        setInterval(this.loadMetrics, 1000)
    }

    loadMetrics = () => {
        AMQPAPI.get('/overview')
            .then(response => this.updateMetrics(response))
    };

    updateMetrics = (response) => {
        let metricsData = response.data.metrics ? response.data.metrics : [];
        let metrics = {};

        metricsData.forEach(function (metric) {
            if (metric.name === 'server.traffic_in' || metric.name === 'server.traffic_out') {
                metric.sample.map((sample) => {
                    sample.value = Math.round(sample.value * 100 / 1024 / 1024) / 100
                })
            }
            metrics[metric.name] = metric.sample
        });
        this.setState({
            metrics: metrics,
            counters: response.data.counters,
        })
    };

    renderLegend = (props) => {
        const {payload} = props;
        const {classes} = this.props;
        return (
            <div>
                {
                    payload.map((entry, index) => {
                        const {metrics} = this.state;
                        let metric = metrics[entry.value];
                        let last = 0;
                        if (metric) {
                            last = metric[metric.length - 1] ? metric[metric.length - 1].value : 0
                        }

                        last = new Intl.NumberFormat().format(last);

                        return (
                            <div key={`item-${index}`} className={classes.legendItem}>
                                <div className={classes.colorKey} style={{background: entry.color}}>&nbsp;</div>
                                <div style={{float: 'left', width: '100px'}}>{metricLabels[entry.value]}</div>
                                <div style={{float: 'left'}}>{last}</div>
                            </div>
                        )
                    }, this)
                }
            </div>
        );
    };

    renderButtons = () => {
        const {counters} = this.state;
        const {classes} = this.props;

        return (
            <Grid
                container
                direction="row">
                <Button variant="outlined" className={classes.button} component={Link} to='/connections'>
                    Connections: <strong className={classes.buttonCount}>{counters.connections}</strong>
                </Button>
                <Button variant="outlined" className={classes.button} component={Link} to='/channels'>
                    Channels: <strong className={classes.buttonCount}>{counters.channels}</strong>
                </Button>
                <Button variant="outlined" className={classes.button} component={Link} to='/exchanges'>
                    Exchanges: <strong className={classes.buttonCount}>{counters.exchanges}</strong>
                </Button>
                <Button variant="outlined" className={classes.button} component={Link} to='/queues'>
                    Queues: <strong className={classes.buttonCount}>{counters.queues}</strong>
                </Button>
                <Button variant="outlined" className={classes.button}>
                    Consumers: <strong className={classes.buttonCount}>{counters.consumers}</strong>
                </Button>

            </Grid>
        )
    };

    renderQueuedMessagesCharts = () => {
        const {metrics} = this.state;
        const {classes} = this.props;
        return (
            <div className={classes.chartDiv}>
                <Typography variant="subheading" color="inherit" className={classes.chartTitle}>
                    Queued messages, msg
                </Typography>
                <LineChart width={1000} height={300}
                           margin={{top: 5, left: 20, right: 30, bottom: 5}}>
                    <XAxis
                        dataKey="timestamp"
                        domain={['auto', 'auto']}
                        name='Time'
                        tickFormatter={timeStr => moment(timeStr * 1000).format('HH:mm:ss')}
                        type='number'
                        tickCount={15}
                    />
                    <YAxis/>
                    <CartesianGrid strokeDasharray="1 1"/>
                    <Legend
                        verticalAlign="top"
                        layout="vertical"
                        align="right"
                        wrapperStyle={{
                            paddingLeft: "20px",
                            width: "150px"
                        }}
                        content={this.renderLegend}
                    />
                    <Line name="server.ready" type="linear" dataKey="value" stroke="#edc240"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.ready"]}/>
                    <Line name="server.unacked" type="linear" dataKey="value" stroke="#afd8f8"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.unacked"]}/>
                    <Line name="server.total" type="linear" dataKey="value" stroke="#cb4b4b"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.total"]}/>
                </LineChart>
            </div>
        )
    };

    renderMessageRatesCharts = () => {
        const {metrics} = this.state;
        const {classes} = this.props;
        return (
            <div className={classes.chartDiv}>
                <Typography variant="subheading" color="inherit" className={classes.chartTitle}>
                    Message rates, msg/s
                </Typography>
                <LineChart width={1000} height={300}
                           margin={{top: 5, left: 20, right: 30, bottom: 5}}>
                    <XAxis
                        dataKey="timestamp"
                        domain={['auto', 'auto']}
                        name='Time'
                        tickFormatter={timeStr => moment(timeStr * 1000).format('HH:mm:ss')}
                        type='number'
                        tickCount={15}
                    />
                    <YAxis/>
                    <CartesianGrid strokeDasharray="1 1"/>
                    <Legend
                        verticalAlign="top"
                        layout="vertical"
                        align="right"
                        wrapperStyle={{
                            paddingLeft: "20px",
                            width: "150px"
                        }}
                        content={this.renderLegend}
                    />
                    <Line name="server.publish" type="linear" dataKey="value" stroke="#edc240"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.publish"]}/>
                    <Line name="server.deliver" type="linear" dataKey="value" stroke="#9440ed"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.deliver"]}/>
                    <Line name="server.acknowledge" type="linear" dataKey="value" stroke="#aaaaaa"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.acknowledge"]}/>
                    <Line name="server.confirm" type="linear" dataKey="value" stroke="#afd8f8"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.confirm"]}/>
                    <Line name="server.get" type="linear" dataKey="value" stroke="#7c79c3"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.get"]}/>
                </LineChart>
            </div>
        )
    };

    renderTrafficRatesCharts = () => {
        const {metrics} = this.state;
        const {classes} = this.props;
        return (
            <div className={classes.chartDiv}>
                <Typography variant="subheading" color="inherit" className={classes.chartTitle}>
                    Network Traffic rates, Mb/s
                </Typography>
                <LineChart width={1000} height={300}
                           margin={{top: 5, left: 20, right: 30, bottom: 5}}>
                    <XAxis
                        dataKey="timestamp"
                        domain={['auto', 'auto']}
                        name='Time'
                        tickFormatter={timeStr => moment(timeStr * 1000).format('HH:mm:ss')}
                        type='number'
                        tickCount={15}
                    />
                    <YAxis/>
                    <CartesianGrid strokeDasharray="1 1"/>
                    <Legend
                        verticalAlign="top"
                        layout="vertical"
                        align="right"
                        wrapperStyle={{
                            paddingLeft: "20px",
                            width: "150px"
                        }}
                        content={this.renderLegend}
                    />
                    <Line name="server.traffic_in" type="linear" dataKey="value" stroke="#edc240"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.traffic_in"]}/>
                    <Line name="server.traffic_out" type="linear" dataKey="value" stroke="#afd8f8"
                          connectNulls={true} isAnimationActive={false} data={metrics["server.traffic_out"]}/>
                </LineChart>
            </div>
        )
    };

    render() {
        const {classes} = this.props;

        return (
            <Grid container direction="column">
                {this.renderButtons()}

                <Grid container direction="column" justify="flex-start" alignItems="flex-start"
                      className={classes.chartsContainer}>

                    {this.renderQueuedMessagesCharts()}

                    {this.renderMessageRatesCharts()}

                    {this.renderTrafficRatesCharts()}
                </Grid>
            </Grid>
        )
    }
}

export default withStyles(styles)(Overview);