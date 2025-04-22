import pymongo


def write_scheduled_orders_to_mongo(db_manager, logger, dataframes):
    try:
        with logger.context("Writing Schedule"):
            logger.info("Writing the final calculated schedule to MongoDb")
            update_count = 0

            all_orders_df = dataframes.all_orders_df
            scheduled_orders_df = dataframes.scheduled_orders_df
            preparation_intervals_df = dataframes.preparation_intervals_df
            logger.info("Gathered needed DataFrames")

            logger.info(f"Starting update of {len(all_orders_df)} open orders based on {len(scheduled_orders_df)} schedule entries.")

            for index, schedule_row in scheduled_orders_df.iterrows():
                schedule_mongo_id = schedule_row['mongoID']

                match_condition = all_orders_df['_id'].astype(str) == schedule_mongo_id
                comment = "Opmerkingen: "
                weekends_inside_task = schedule_row['weekends_inside']
                if weekends_inside_task:
                    date_strings = [d.strftime('%Y-%m-%d') for d in weekends_inside_task]
                    dates_listed = ", ".join(date_strings)
                    comment = comment + f"Volgende weekend dagen waar er niet geproduceerd wordt vallen binnen deze batch: {dates_listed}, er is een totaal van {len(weekends_inside_task) * 24} uur extra bij de taak gerekend"

                order_indices = all_orders_df.index[match_condition]

                all_orders_df.loc[order_indices, 'resourceId'] = schedule_row['machineID']
                all_orders_df.loc[order_indices, 'duration'] = schedule_row['duration']
                all_orders_df.loc[order_indices, 'startDate'] = schedule_row['startTime']
                all_orders_df.loc[order_indices, 'endDate'] = schedule_row['endTime']
                all_orders_df.loc[order_indices, 'comment'] = comment

                #Temp for dev reasons
                all_orders_df.loc[order_indices, 'purchaseID'] = schedule_row['mongoID']
                update_count += len(order_indices)
            logger.info("For every open order changes the resourceId, duration, startDate and endDate to match that of the solver in the DataFrame")

            records_to_insert = []
            logger.info("Gathering all of the preparation time intervals from the solver")
            for index, schedule_row in preparation_intervals_df.iterrows():
                interval = {
                    "stilstand" : 1,
                    "uren": schedule_row['duration'],
                    "baseDuration": schedule_row['duration'],
                    "resourceId": schedule_row['machineID'],
                    "startDate": schedule_row['startTime'],
                    "endDate": schedule_row['endTime'],
                    "groupId": -1,
                    "matrijs": -1,
                    "opmerking": schedule_row['reason'],
                    "ombouwRef": schedule_row['mongoID'],
                    "removed": 0,
                    "splitAantal": None,
                    "comment": "",
                    "durationUnit": "h",
                    "parentId": schedule_row['machineID'],
                    "description": schedule_row['reason'],
                    "eventColor": "indigo",
                    "edited": False,
                    "opstart": "01/01/2100",
                    "leverDatum": "01/01/2100"
                }
                records_to_insert.append(interval)
            logger.info(f"Finished adding {len(preparation_intervals_df)} preparation intervals to their list")

            try:
                records_to_insert.extend(all_orders_df.to_dict('records'))
                logger.info("Turned all of the orders into a dict")

                db_manager.ai_planning_suggestion.delete_many({})
                logger.info(f"Deleted all orders from the ai suggestion mongo collection")

                insert_result = db_manager.ai_planning_suggestion.insert_many(records_to_insert, ordered=False)
                inserted_count = len(insert_result.inserted_ids)
                logger.info(f"Successfully inserted {inserted_count} new documents into AI planning suggestion collection.")

                if inserted_count != len(records_to_insert):
                    logger.error(f"Mismatch: Prepared {len(records_to_insert)} records but MongoDB reported inserting {inserted_count}.")

            except Exception as e:
                logger.error(f"Error inserting new documents into AI planning suggestion collection from DataFrame: {e}")

    except Exception as e:
        logger.error(f"Error during update of open orders DataFrame: {e}")
        return all_orders_df