class UnitConverter {
    round(n, unit) {
        if (unit in this.decimalPlaces) {
            return [Math.round(n * Math.pow(10, this.decimalPlaces[unit])) / Math.pow(10, this.decimalPlaces[unit]), unit];
        }

        return [n, unit];
    }
}

export class Imperial extends UnitConverter {
    decimalPlaces = {
        'F': 0,
        'ft': 0,
        'mph': 0,
        'inHg': 2,
    };

    convert(val, unit) {
        switch (unit) {
            case 'K':
                return this.round(((val - 273.15) * 1.8) + 32, 'F');
            case 'm':
                return this.round(val * 3.2808, 'ft');
            case 'm/s':
                return this.round(val * 2.237, 'mph');
            case 'Pa':
                return this.round(val * 0.0002953, 'inHg');
            default:
                return this.round(val, unit);
        }
    }
}

export class Metric extends UnitConverter{
    decimalPlaces = {
        'C': 0,
        'm': 0,
        'm/s': 0,
        'Pa': 2,
    };

    convert(val, unit) {
        switch (unit) {
            case 'K':
                return this.round(val - 273.15, 'C');
            default:
                return this.round(val, unit);
        }
    }
}
