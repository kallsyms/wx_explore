import axios from 'axios';

export default axios.create({
    baseURL: "https://api.vortexweather.tech/api",
    timeout: 5000,
});
