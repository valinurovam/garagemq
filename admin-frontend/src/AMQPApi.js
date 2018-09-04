import axios from "axios/index";

const AMQPAPI = axios.create({
    baseURL: (process.env.NODE_ENV === 'development') ? 'http://localhost:15672' : '',
    headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    },
});

export default AMQPAPI;