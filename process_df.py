def process_dataframe(table_name: str, df: pd.DataFrame):
    for col_name in df.keys().values:
        current_dtype = df[col_name].dtype
        current_first_item = _get_first_item(df, col_name)
        current_item_type = type(current_first_item)
        
        # Debugging
        print(f"DJ: current column name={col_name}")
        print(f"DJ: current column df[col_name]={df[col_name]}")
        print(f"DJ: current_dtype={current_dtype}")
        print(f"DJ: current_first_item={current_first_item}")
        print(f"DJ: current_item_type={current_item_type}")

        processed_col_name = schemas.find_column_renaming(table_name, col_name)
        schema_of_this_column = schemas.get_table_column_schema(table_name, col_name)
        print(f"DJ: processed_col_name={processed_col_name}")
        print(f"DJ: schema_of_this_column={schema_of_this_column}")

        if not processed_col_name and not schema_of_this_column:
            # New column, process it and append schema
            schema_of_this_column = init_column_schema(
                current_dtype, current_first_item
            )
            processed_col_name = process_column_name(col_name)
            if processed_col_name != col_name:
                schemas.add_column_renaming(table_name, col_name, processed_col_name)
            schemas.append_schema_column(
                table_name, processed_col_name, schema_of_this_column
            )

        if processed_col_name and processed_col_name != col_name:
            df.rename(columns={col_name: processed_col_name}, inplace=True)
            col_name = processed_col_name

        # Ensure all rows in the column conform to the schema
        expected_type = schema_of_this_column[TYPE_KEY]
        if not all(isinstance(item, expected_type) or pd.isnull(item) for item in df[col_name]):
            logger.debug(
                f"Column {col_name} contains items that do not match the expected type {expected_type}. Applying conversion."
            )
            df[col_name] = df[col_name].apply(
                TYPE_TO_CONVERT_FUNCTION_MAP.get(expected_type, do_nothing)
            )
            print(f"DJ: Corrected df[col_name]={df[col_name]}")

        logger.debug(f"current_dtype={current_dtype}")
        logger.debug(
            f"schema_of_this_column[DTYPE_KEY]={schema_of_this_column[DTYPE_KEY]}"
        )
        if current_dtype != schema_of_this_column[DTYPE_KEY]:
            try:
                logger.debug(
                    f"different column dtype detected: current_dtype={current_dtype}, item type from schema={schema_of_this_column[DTYPE_KEY]}"
                )
                df[col_name] = df[col_name].astype(schema_of_this_column[DTYPE_KEY])
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"An {e.__class__.__name__} was caught when trying to convert "
                    + f"the dtype of the column {col_name} from {current_dtype} to {schema_of_this_column[DTYPE_KEY]}"
                )