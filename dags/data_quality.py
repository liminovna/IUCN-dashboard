import pandas as pd

class dataQualityReport:
    def __init__(self, airflow_data, data, checks: list):
        """
        Expected `checks` structure:
        [
            {'check_name': 'uniqueness', 'critical': True, 'check_subset': ['col1', 'col2']},
            {'check_name': 'completeness', 'critical': False, 'check_subset': 'col1'},
            {'check_name': 'completeness', 'critical': False, 'check_subset': 'col2'},
        ]
        """
        self.airflow_data = airflow_data

        self.critical_errors = False
        self.noncritical_errors = False

        assert isinstance(data, dict) or isinstance(data, pd.DataFrame), 'Unsupported data type of data!'
        self.data = data

        rows = []
        for criterion in checks:
            rows.append(self.check_criterion_(criterion))

        self.data_quality_checks_df = pd.DataFrame(rows)
        
    def check_criterion_(self, criterion_params: dict):
        """
        Run data quality check. Form a template and update it with the output obtained during the data check.
        If any critical data checks failed, continue returning templates for the later data checks, but do not run the checks themselves.
        """
        # template
        check_subset_ = criterion_params.get('check_subset')
        row_data = {
            'check': criterion_params['check_name'], 
            'factual_value_int': None, # output, optional
            'factual_value_str': None, # output, optional
            'check_subset': str(check_subset_), # optional  
            'status': None, # output
            'critical': criterion_params['critical'], 
            'comment': None # output, optional
        }
        # if not self.critical_errors:
        if True:
            try:
                check_function = getattr(self, criterion_params['check_name'])
                try:
                    check_result = check_function(**criterion_params)

                    row_data['status'] = check_result['status']
                    row_data['factual_value_int'] = check_result.get('factual_value_int')
                    row_data['factual_value_str'] = check_result.get('factual_value_str')
                    row_data['comment'] = check_result.get('comment')
                    
                except Exception as e:
                    row_data['status'] = 'error'
                    row_data['comment'] = str(e)
                    # return row_data
            except AttributeError:
                row_data['status'] = 'unsupported'
                row_data['comment'] = 'Unsupported criterion!'
                # return row_data
            
            if row_data['status'] != 'ok':
                if criterion_params['critical'] == True:
                    self.critical_errors = True
                elif criterion_params['critical'] == False:
                    self.noncritical_errors = True
        # else:
        #     row_data['comment'] = 'Could not run due to previous critical errors!'

        return row_data

    def record_count(self, **criterion_params):
        """
        Counts records or rows. If data is an API response JSON, look inside `main_json_key`

        Supported data types for data:
        - dict (JSON): pass dct[main_key]
        - pandas DataFrame

        Example:
        {'check': 'record_count', 'critical': True}
        """
        count = len(self.data)
        
        return {'status': 'ok' if count > 0 else 'failed', 'factual_value_int': count}

    def required_cols_present(self, **criterion_params):
        """
        Checks for the columns specified by `expected_values`. If data is an API response JSON, look inside `main_json_key`.

        Supported data types for data:
        - dict (JSON): pass dct[main_key]
        - pandas DataFrame

        Required parameters:
        - `check_subset`

        Example:
        {'check': 'required_cols_present', 'critical': True, 'check_subset': ['description', 'code']}
        """
        if isinstance(self.data, pd.DataFrame):
            fact_columns = self.data.columns

        if isinstance(self.data, dict):
            fact_columns = self.data[0].keys()

        condition = all([c in fact_columns for c in criterion_params['check_subset']])
        return {'status': 'ok' if condition else 'failed', 'factual_value_str': str(list(fact_columns))}

    def uniqueness(self, **criterion_params):
        """
        Check for uniqueness in the primary key. 
        To check multiple columns separately, pass parameters for the checks in separate dictionaries

        Supported data types for data:
        - pandas DataFrame

        - `check_subset`: list

        Example:
        {'check': uniqueness, 'critical': True, 'check_subset': ['description', 'code']}
        """
        assert isinstance(self.data, pd.DataFrame), 'Unsupported data type!'
        assert 'check_subset' in criterion_params, 'Please pass parameter check_subset to run uniqueness'

        duplicated_count = len(self.data[self.data.duplicated(subset=criterion_params['check_subset'])])
        if duplicated_count == 0:
            return {'status': 'ok'}
        return {'status': 'failed', 'comment': f' Found {duplicated_count} duplicated rows'}
        
    def completeness(self, **criterion_params):
        """
        Check for missing values in a column specified in `check_subset`.

        Supported data types for data:
        - pandas DataFrame

        - `check_subset`: str

        Example:
        {'check': completeness, 'critical': True, 'check_subset': 'description'}
        """
        assert isinstance(self.data, pd.DataFrame), 'Unsupported data type!'
        assert 'check_subset' in criterion_params, 'Please pass parameter check_column to run completeness'
        assert isinstance(criterion_params['check_subset'], str), '`check_subset` must be a string!'

        missing_count = self.data[criterion_params['check_subset']].isna().sum()
        if missing_count == 0:
            return {'status': 'ok'}
        return {'status': 'failed', 'comment': f' Found {missing_count} ({round(missing_count/len(self.data)*100, 2)}%) missing values'}
