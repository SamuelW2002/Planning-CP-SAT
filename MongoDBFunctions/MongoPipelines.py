from datetime import date, datetime, time
import re


def all_technicians_pipeline():
    pipeline = [
        {
            '$match': {
                'resourceId': -1
            }
        },
        {
            '$addFields': {
                'startDate': {
                    '$dateFromString': {
                        'dateString': '$startDate',
                        'format': '%Y-%m-%dT%H:%M'
                    }
                },
                'endDate': {
                    '$dateFromString': {
                        'dateString': '$endDate',
                        'format': '%Y-%m-%dT%H:%M'
                    }
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'startDate': 1,
                'endDate': 1,
                'ombouwersBeschikbaar': 1
            }
        }
    ]
    return pipeline


# Stilstand 0 is all orders op machines en hun ombouw intervallen
# Zonder het woord ombouw in de opmerking is het enkel de orders
# Resource -2 is op externe machines gepland dus niet meerekenen
def all_orders_pipeline():
    pipeline = [
        {
            '$match': {
                'stilstand': 0,
                'opmerking': {
                    '$not': re.compile(r'ombouw', re.IGNORECASE)
                },
                'resourceId': {
                    '$ne': -2
                }
            }
        },
        {
            '$lookup': {
                'from': "matrijzen",
                'localField': 'matrijs',
                'foreignField': '_id',
                'as': 'joined_matrijs_data'
            }
        },
        {
            '$unwind': {
                'path': '$joined_matrijs_data',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$addFields': {
                'subserieID': '$joined_matrijs_data.subserieID',
                'matrijsName': '$joined_matrijs_data.name',
                'hotrunner': '$joined_matrijs_data.hotrunner'
            }
        },
        {
            '$project': {
                'joined_matrijs_data': 0
            }
        }
    ]
    return pipeline

def machine_unavailable_pipeline():
    today_start_dt = datetime.combine(date.today(), time.min)
    pipeline = [
        {
            '$match': {
                'stilstand': 1,
                'opmerking': {
                    '$not': re.compile(r'ombouw', re.IGNORECASE)
                },
                '$expr': {
                    '$and': [
                        {'$ne': [{'$type': '$startDate'}, 'missing']},
                        {'$ne': ['$startDate', None]},
                        {'$eq': [{'$type': '$startDate'}, 'string']},
                        # Convert string to date and compare
                        {
                            '$gte': [
                                {
                                    '$toDate': '$startDate'
                                },
                                today_start_dt
                            ]
                        }
                    ]
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'startDate': 1, 
                'endDate': 1,
                'resourceId': 1,
            }
        }
    ]
    return pipeline