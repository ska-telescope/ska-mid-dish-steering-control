#Feed Indexer tests [316-000000-043]

#Author: P.P.A. Kotze
#Date: 1/9/2020
#Version: 
#0.1 Initial
#0.2 Update after feedback and correction from HN email dated 1/8/2020
#0.3 Rework scu_get and scu_put to simplify
#0.4 attempt more generic scu_put with either jason payload or simple params, remove payload from feedback function
#0.5 create scu_lib
#0.6 1/10/2020 added load track tables and start table tracking also as debug added 'field' command for old scu
#HN: 13/05/2021 Changed the way file name is defined by defining a start time
##: 07/10/2021 Added new "save_session14" where file time is no longer added to the file name in this library, but expected to be passed as part of "filename" string argument from calling script.

#Import of Python available libraries
import time
import requests
import json

#scu_ip = '10.96.64.10'
port = '8080'

#define some preselected sensors for recording into a logfile
hn_feed_indexer_sensors=[
'acu.time.act_time_source',
'acu.time.internal_time',
'acu.time.external_ptp',
'acu.general_management_and_controller.state',
'acu.general_management_and_controller.feed_indexer_pos',
'acu.azimuth.state',
'acu.azimuth.p_set',
'acu.azimuth.p_act',
'acu.azimuth.v_act',
'acu.elevation.state',
'acu.elevation.p_set',
'acu.elevation.p_act',
'acu.elevation.v_act',
'acu.feed_indexer.state',
'acu.feed_indexer.p_set',
'acu.feed_indexer.p_shape',
'acu.feed_indexer.p_act',
'acu.feed_indexer.v_shape',
'acu.feed_indexer.v_act',
'acu.feed_indexer.motor_1.actual_velocity',
'acu.feed_indexer.motor_2.actual_velocity',
'acu.feed_indexer.motor_1.actual_torque',
'acu.feed_indexer.motor_2.actual_torque',
'acu.general_management_and_controller.act_power_consum',
'acu.general_management_and_controller.power_factor',
'acu.general_management_and_controller.voltage_phase_1',
'acu.general_management_and_controller.voltage_phase_2',
'acu.general_management_and_controller.voltage_phase_3',
'acu.general_management_and_controller.current_phase_1',
'acu.general_management_and_controller.current_phase_2',
'acu.general_management_and_controller.current_phase_3'
]

#hn_tilt_sensors is equivalent to "Servo performance"
hn_tilt_sensors=[
'acu.time.act_time_source',
'acu.time.internal_time',
'acu.time.external_ptp',
'acu.general_management_and_controller.state',
'acu.general_management_and_controller.feed_indexer_pos',
'acu.azimuth.state',
'acu.azimuth.p_set',
'acu.azimuth.p_act',
'acu.azimuth.v_act',
'acu.elevation.state',
'acu.elevation.p_set',
'acu.elevation.p_act',
'acu.elevation.v_act',
'acu.general_management_and_controller.act_power_consum',
'acu.general_management_and_controller.power_factor',
'acu.general_management_and_controller.voltage_phase_1',
'acu.general_management_and_controller.voltage_phase_2',
'acu.general_management_and_controller.voltage_phase_3',
'acu.general_management_and_controller.current_phase_1',
'acu.general_management_and_controller.current_phase_2',
'acu.general_management_and_controller.current_phase_3',
'acu.pointing.act_amb_temp_1',
'acu.pointing.act_amb_temp_2',
'acu.pointing.act_amb_temp_3',
'acu.general_management_and_controller.temp_air_inlet_psc',
'acu.general_management_and_controller.temp_air_outlet_psc',
'acu.pointing.incl_signal_x_raw',
'acu.pointing.incl_signal_x_deg',
'acu.pointing.incl_signal_x_filtered',
'acu.pointing.incl_signal_x_corrected',
'acu.pointing.incl_signal_y_raw',
'acu.pointing.incl_signal_y_deg',
'acu.pointing.incl_signal_y_filtered',
'acu.pointing.incl_signal_y_corrected',
'acu.pointing.incl_temp',
'acu.pointing.incl_corr_val_az',
'acu.pointing.incl_corr_val_el'
]

class scu():
    def __init__(self):
        self.ip = 'localhost'
        self.port = '8080'
        self.debug = True

    #Direct SCU webapi functions based on urllib PUT/GET
    def feedback(self, r):
        if self.debug == True:
            print('***Feedback:', r.request.url, r.request.body)
            print(r.reason, r.status_code)
            print("***Text returned:")
            print(r.text)
        elif r.status_code != 200:
            print('***Feedback:', r.request.url, r.request.body)
            print(r.reason, r.status_code)
            print("***Text returned:")
            print(r.text)
            #print(r.reason, r.status_code)
            #print()

    #	def scu_get(device, params = {}, r_ip = self.ip, r_port = port):
    def scu_get(self, device, params = {}):
        '''This is a generic GET command into http: scu port + folder 
        with params=payload'''
        URL = 'http://' + self.ip + ':' + self.port + device
        r = requests.get(url = URL, params = params)
        self.feedback(r)
        return(r)

    def scu_put(self, device, payload = {}, params = {}, data=''):
        '''This is a generic PUT command into http: scu port + folder 
        with json=payload'''
        URL = 'http://' + self.ip + ':' + self.port + device
        r = requests.put(url = URL, json = payload, params = params, data = data)
        self.feedback(r)
        return(r)

    def scu_delete(self, device, payload = {}, params = {}):
        '''This is a generic DELETE command into http: scu port + folder 
        with params=payload'''
        URL = 'http://' + self.ip + ':' + self.port + device
        r = requests.delete(url = URL, json = payload, params = params)
        self.feedback(r)
        return(r)

    #SIMPLE PUTS

    #command authority
    def command_authority(self, action):
        #1 get #2 release
        print('command authority: ', action)
        authority={'Get': 1, 'Release': 2}
        self.scu_put('/devices/command', 
            {'path': 'acu.command_arbiter.authority',
            'params': {'action': authority[action]}})

    #commands to DMC state - dish management controller
    def interlock_acknowledge_dmc(self):
        print('reset dmc')
        self.scu_put('/devices/command',
            {'path': 'acu.dish_management_controller.interlock_acknowledge'})

    def reset_dmc(self):
        print('reset dmc')
        self.scu_put('/devices/command', 
            {'path': 'acu.dish_management_controller.reset'})

    def activate_dmc(self):
        print('activate dmc')
        self.scu_put('/devices/command',
            {'path': 'acu.dish_management_controller.activate'})

    def deactivate_dmc(self):
        print('deactivate dmc')
        self.scu_put('/devices/command', 
            {'path': 'acu.dish_management_controller.deactivate'})

    def move_to_band(self, position):
        bands = {'Band 1': 1, 'Band 2': 2, 'Band 3': 3, 'Band 4': 4, 'Band 5a': 5, 'Band 5b': 6, 'Band 5c': 7}
        print('move to band:', position)
        if not(isinstance(position, str)):
            self.scu_put('/devices/command',
            {'path': 'acu.dish_management_controller.move_to_band',
             'params': {'action': position}})
        else:
            self.scu_put('/devices/command',
            {'path': 'acu.dish_management_controller.move_to_band',
             'params': {'action': bands[position]}})
            
    def abs_azel(self, az_angle, el_angle):
        print('abs az: {:.4f} el: {:.4f}'.format(az_angle, el_angle))
        self.scu_put('/devices/command',
            {'path': 'acu.dish_management_controller.slew_to_abs_pos',
             'params': {'new_azimuth_absolute_position_set_point': az_angle,
                   'new_elevation_absolute_position_set_point': el_angle}})

    #commands to ACU
    def activate_az(self):
        print('act azimuth')
        self.scu_put('/devices/command', 
            {'path': 'acu.elevation.activate'})

    def activate_el(self):
        print('activate elevation')
        self.scu_put('/devices/command', 
            {'path': 'acu.elevation.activate'})

    def deactivate_el(self):
        print('deactivate elevation')
        self.scu_put('/devices/command', 
            {'path': 'acu.elevation.deactivate'})

    def abs_azimuth(self, az_angle, az_vel):
        print('abs az: {:.4f} vel: {:.4f}'.format(az_angle, az_vel))
        self.scu_put('/devices/command',
            {'path': 'acu.azimuth.slew_to_abs_pos',
             'params': {'new_axis_absolute_position_set_point': az_angle,
              'new_axis_speed_set_point_for_this_move': az_vel}})    

    def abs_elevation(self, el_angle, el_vel):
        print('abs el: {:.4f} vel: {:.4f}'.format(el_angle, el_vel))
        self.scu_put('/devices/command',
            {'path': 'acu.elevation.slew_to_abs_pos',
             'params': {'new_axis_absolute_position_set_point': el_angle,
              'new_axis_speed_set_point_for_this_move': el_vel}}) 

    def load_static_offset(self, az_offset, el_offset):
        print('offset az: {:.4f} el: {:.4f}'.format(az_offset, el_offset))
        self.scu_put('/devices/command',
            {'path': 'acu.tracking_controller.load_static_tracking_offsets.',
             'params': {'azimuth_tracking_offset': az_offset,
                        'elevation_tracking_offset': el_offset}})     #Track table commands


    
    def load_program_track(self, load_type, entries, t=[0]*50, az=[0]*50, el=[0]*50):
        print(load_type)    
        LOAD_TYPES = {
            'LOAD_NEW' : 1, 
            'LOAD_ADD' : 2, 
            'LOAD_RESET' : 3}
        
        #table selector - to tidy for future use
        ptrackA = 11
        
        TABLE_SELECTOR =  {
            'pTrackA' : 11,
            'pTrackB' : 12,
            'oTrackA' : 21,
            'oTrackB' : 22}
        
        #funny thing is SCU wants 50 entries, even for LOAD RESET! or if you send less then you have to pad the table
       
        if entries != 50:
            padding = 50 - entries
            t  += [0] * padding
            az += [0] * padding
            el += [0] * padding

        self.scu_put('/devices/command',
                     {'path': 'acu.dish_management_controller.load_program_track',
                      'params': {'table_selector': ptrackA,
                                 'load_mode': LOAD_TYPES[load_type],
                                 'number_of_transmitted_program_track_table_entries': entries,
                                 'time_0': t[0], 'time_1': t[1], 'time_2': t[2], 'time_3': t[3], 'time_4': t[4], 'time_5': t[5], 'time_6': t[6], 'time_7': t[7], 'time_8': t[8], 'time_9': t[9], 'time_10': t[10], 'time_11': t[11], 'time_12': t[12], 'time_13': t[13], 'time_14': t[14], 'time_15': t[15], 'time_16': t[16], 'time_17': t[17], 'time_18': t[18], 'time_19': t[19], 'time_20': t[20], 'time_21': t[21], 'time_22': t[22], 'time_23': t[23], 'time_24': t[24], 'time_25': t[25], 'time_26': t[26], 'time_27': t[27], 'time_28': t[28], 'time_29': t[29], 'time_30': t[30], 'time_31': t[31], 'time_32': t[32], 'time_33': t[33], 'time_34': t[34], 'time_35': t[35], 'time_36': t[36], 'time_37': t[37], 'time_38': t[38], 'time_39': t[39], 'time_40': t[40], 'time_41': t[41], 'time_42': t[42], 'time_43': t[43], 'time_44': t[44], 'time_45': t[45], 'time_46': t[46], 'time_47': t[47], 'time_48': t[48], 'time_49': t[49],
                                 'azimuth_position_0': az[0], 'azimuth_position_1': az[1], 'azimuth_position_2': az[2], 'azimuth_position_3': az[3], 'azimuth_position_4': az[4], 'azimuth_position_5': az[5], 'azimuth_position_6': az[6], 'azimuth_position_7': az[7], 'azimuth_position_8': az[8], 'azimuth_position_9': az[9], 'azimuth_position_10': az[10], 'azimuth_position_11': az[11], 'azimuth_position_12': az[12], 'azimuth_position_13': az[13], 'azimuth_position_14': az[14], 'azimuth_position_15': az[15], 'azimuth_position_16': az[16], 'azimuth_position_17': az[17], 'azimuth_position_18': az[18], 'azimuth_position_19': az[19], 'azimuth_position_20': az[20], 'azimuth_position_21': az[21], 'azimuth_position_22': az[22], 'azimuth_position_23': az[23], 'azimuth_position_24': az[24], 'azimuth_position_25': az[25], 'azimuth_position_26': az[26], 'azimuth_position_27': az[27], 'azimuth_position_28': az[28], 'azimuth_position_29': az[29], 'azimuth_position_30': az[30], 'azimuth_position_31': az[31], 'azimuth_position_32': az[32], 'azimuth_position_33': az[33], 'azimuth_position_34': az[34], 'azimuth_position_35': az[35], 'azimuth_position_36': az[36], 'azimuth_position_37': az[37], 'azimuth_position_38': az[38], 'azimuth_position_39': az[39], 'azimuth_position_40': az[40], 'azimuth_position_41': az[41], 'azimuth_position_42': az[42], 'azimuth_position_43': az[43], 'azimuth_position_44': az[44], 'azimuth_position_45': az[45], 'azimuth_position_46': az[46], 'azimuth_position_47': az[47], 'azimuth_position_48': az[48], 'azimuth_position_49': az[49],
                                 'elevation_position_0': el[0], 'elevation_position_1': el[1], 'elevation_position_2': el[2], 'elevation_position_3': el[3], 'elevation_position_4': el[4], 'elevation_position_5': el[5], 'elevation_position_6': el[6], 'elevation_position_7': el[7], 'elevation_position_8': el[8], 'elevation_position_9': el[9], 'elevation_position_10': el[10], 'elevation_position_11': el[11], 'elevation_position_12': el[12], 'elevation_position_13': el[13], 'elevation_position_14': el[14], 'elevation_position_15': el[15], 'elevation_position_16': el[16], 'elevation_position_17': el[17], 'elevation_position_18': el[18], 'elevation_position_19': el[19], 'elevation_position_20': el[20], 'elevation_position_21': el[21], 'elevation_position_22': el[22], 'elevation_position_23': el[23], 'elevation_position_24': el[24], 'elevation_position_25': el[25], 'elevation_position_26': el[26], 'elevation_position_27': el[27], 'elevation_position_28': el[28], 'elevation_position_29': el[29], 'elevation_position_30': el[30], 'elevation_position_31': el[31], 'elevation_position_32': el[32], 'elevation_position_33': el[33], 'elevation_position_34': el[34], 'elevation_position_35': el[35], 'elevation_position_36': el[36], 'elevation_position_37': el[37], 'elevation_position_38': el[38], 'elevation_position_39': el[39], 'elevation_position_40': el[40], 'elevation_position_41': el[41], 'elevation_position_42': el[42], 'elevation_position_43': el[43], 'elevation_position_44': el[44], 'elevation_position_45': el[45], 'elevation_position_46': el[46], 'elevation_position_47': el[47], 'elevation_position_48': el[48], 'elevation_position_49': el[49]}})

    def start_program_track(self, start_time):
        ptrackA = 11
        #interpol_modes
        NEWTON = 0
        SPLINE = 1
        #start_track_modes
        AZ_EL = 1
        RA_DEC = 2
        RA_DEC_SC = 3  #shortcut
        self.scu_put('/devices/command',
                     {'path': 'acu.dish_management_controller.start_program_track',
                      'params' : {'table_selector': ptrackA,
                                  'start_time_mjd': start_time,
                                  'interpol_mode': SPLINE,
                                  'track_mode': AZ_EL }})
    
    def acu_ska_track(self, BODY):
        print('acu ska track')
        self.scu_put('/acuska/programTrack', 
                data = BODY)
        
    def acu_ska_track_stoploadingtable(self):
        print('acu ska track stop loading table')
        self.scu_put('/acuska/stopLoadingTable')
        
    def format_tt_line(self, t, az,  el, capture_flag = 1, parallactic_angle = 0.0):
        '''something will provide a time, az and el as minimum
        time must alread be absolute time desired in mjd format
        assumption is capture flag and parallactic angle will not be used'''
        f_str='{:.12f} {:.6f} {:.6f} {:.0f} {:.6f} \n'.format(float(t), float(az), float(el), capture_flag, float(parallactic_angle))
        return(f_str)

    def format_body(self, t, az, el):
        body = ''
        for i in range(len(t)):
            body += self.format_tt_line(t[i], az[i], el[i])
        return(body)        

    #status get functions goes here
    
    def status_Value(self, sensor):
        r=self.scu_get('/devices/statusValue', 
              {'path': sensor})
        data = r.json()['value']
        #print('value: ', data)
        return(data)

    def status_finalValue(self, sensor):
        #print('get status finalValue: ', sensor)
        r=self.scu_get('/devices/statusValue', 
              {'path': sensor})
        data = r.json()['finalValue']
        #print('finalValue: ', data)
        return(data)

    def commandMessageFields(self, commandPath):
        r=self.scu_get('/devices/commandMessageFields', 
              {'path': commandPath})
        return(r)

    def statusMessageField(self, statusPath):
        r=self.scu_get('/devices/statusMessageFields', 
              {'deviceName': statusPath})
        return(r)
    
    #ppak added 1/10/2020 as debug for onsite SCU version
    #but only info about sensor, value itself is murky?
    def field(self, sensor):
        #old field method still used on site
        r=self.scu_get('/devices/field', 
              {'path': sensor})
        #data = r.json()['value']
        data = r.json()
        return(data)
    
    #logger functions goes here

    def create_logger(self, config_name, sensor_list):
        '''
        PUT create a config for logging
        Usage:
        create_logger('HN_INDEX_TEST', hn_feed_indexer_sensors)
        or 
        create_logger('HN_TILT_TEST', hn_tilt_sensors)
        '''
        print('create logger')
        r=self.scu_put('/datalogging/config', 
              {'name': config_name,
               'paths': sensor_list})
        return(r)

    '''unusual does not take json but params'''
    def start_logger(self, filename):
        print('start logger: ', filename)
        r=self.scu_put('/datalogging/start',
              params='configName=' + filename)
        return(r)

    def stop_logger(self):
        print('stop logger')
        r=self.scu_put('/datalogging/stop')
        return(r)

    def logger_state(self):
#        print('logger state ')
        r=self.scu_get('/datalogging/currentState')
        #print(r.json()['state'])
        return(r.json()['state'])

    def logger_configs(self):
        print('logger configs ')
        r=self.scu_get('/datalogging/configs')
        return(r)

    def last_session(self):
        '''
        GET last session
        '''
        print('Last sessions ')
        r=self.scu_get('/datalogging/lastSession')
        session = (r.json()['uuid'])
        return(session)
    
    def logger_sessions(self):
        '''
        GET all sessions
        '''
        print('logger sessions ')
        r=self.scu_get('/datalogging/sessions')
        return(r)

    def session_query(self, id):
        '''
        GET specific session only - specified by id number
        Usage:
        session_query('16')
        '''
        print('logger sessioN query id ')
        r=self.scu_get('/datalogging/session',
             {'id': id})
        return(r)

    def session_delete(self, id):
        '''
        DELETE specific session only - specified by id number
        Not working - returns response 500
        Usage:
        session_delete('16')
        '''
        print('delete session ')
        r=self.scu_delete('/datalogging/session',
             params= 'id='+id)
        return(r)

    def session_rename(self, id, new_name):
        '''
        RENAME specific session only - specified by id number and new session name
        Not working
        Works in browser display only, reverts when browser refreshed!
        Usage:
        session_rename('16','koos')
        '''    
        print('rename session ')
        r=self.scu_put('/datalogging/session',
             params = {'id': id, 
                'name' : new_name})
        return(r)


    def export_session(self, id, interval_ms=1000):
        '''
        EXPORT specific session - by id and with interval
        output r.text could be directed to be saved to file 
        Usage: 
        export_session('16',1000)
        or export_session('16',1000).text 
        '''
        print('export session ')
        r=self.scu_get('/datalogging/exportSession',
             params = {'id': id, 
                'interval_ms' : interval_ms})
        return(r)

    #sorted_sessions not working yet

    def sorted_sessions(self, isDescending = 'True', startValue = '1', endValue = '25', sortBy = 'Name', filterType='indexSpan'):
        print('sorted sessions')
        r=self.scu_get('/datalogging/sortedSessions',
             {'isDescending': isDescending,
              'startValue': startValue,
              'endValue': endValue,
              'filterType': filterType, #STRING - indexSpan|timeSpan,
              'sortBy': sortBy})
        return(r)

    #get latest session
    def save_session(self, filename, interval_ms=1000, session = 'last'):
        '''
        Save session data as CSV after EXPORTing it
        Default interval is 1s
        Default is last recorded session
        if specified no error checking to see if it exists
        Usage: 
        export_session('16',1000)
        or export_session('16',1000).text 
        '''
        from pathlib import Path
        print('Attempt export and save of session: {} at rate {:.0f} ms'.format(session, interval_ms))
        if session == 'last':
            #get all logger sessions, may be many
            r=self.logger_sessions()
            #[-1] for end of list, and ['uuid'] to get id of last session in list
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        print('Session id: {} '.format(session))
        file_time = str(int(time.time()))
        file_name = str(filename + '_' + file_time + '.csv')
        file_path = Path.cwd()  / 'output' / file_name
        print('Log file location:', file_path)    
        f = open(file_path, 'a+')
        f.write(file_txt)
        f.close()

    #get latest session ADDED BY HENK FOR BETTER FILE NAMING FOR THE OPTICAL TESTS (USING "START" AS TIMESTAMP)
    def save_session13(self, filename, start, interval_ms=1000, session = 'last'):
        '''
        Save session data as CSV after EXPORTing it
        Default interval is 1s
        Default is last recorded session
        if specified no error checking to see if it exists
        Usage: 
        export_session('16',1000)
        or export_session('16',1000).text 
        '''
        from pathlib import Path
        print('Attempt export and save of session: {} at rate {:.0f} ms'.format(session, interval_ms))
        if session == 'last':
            #get all logger sessions, may be many
            r=self.logger_sessions()
            #[-1] for end of list, and ['uuid'] to get id of last session in list
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        print('Session id: {} '.format(session))
##        file_time = str(int(time.time()))
        file_time = str(int(start))
        file_name = str(filename + '_' + file_time + '.csv')
        file_path = Path.cwd()  / 'output' / file_name
        print('Log file location:', file_path)    
        f = open(file_path, 'a+')
        f.write(file_txt)
        f.close()
        
    def save_session14(self, filename, interval_ms=1000, session = 'last'):
        '''
        Save session data as CSV after EXPORTing it
        Default interval is 1s
        Default is last recorded session
        if specified no error checking to see if it exists
        Usage: 
        export_session('16',1000)
        or export_session('16',1000).text 
        '''
        from pathlib import Path
        print('Attempt export and save of session: {} at rate {:.0f} ms'.format(session, interval_ms))
        if session == 'last':
            #get all logger sessions, may be many
            r=self.logger_sessions()
            session = self.last_session()
        file_txt = self.export_session(session, interval_ms).text
        print('Session id: {} '.format(session))
        file_name = str(filename + '.csv')
        file_path = Path.cwd()  / 'output' / file_name
        print('Log file location:', file_path)    
        f = open(file_path, 'a+')
        f.write(file_txt)
        f.close()
        
    #Simplified one line commands particular to test section being peformed 

    #wait seconds, wait value, wait finalValue
    def wait_duration(self, seconds):
        print('  wait for {:.1f}s'.format(seconds), end="")
        time.sleep(seconds)
        print(' done *')

    def wait_value(self, sensor, value):
        print('wait until sensor: {} == value {}'.format(sensor, value))
        while status_Value(sensor) != value:
            time.sleep(1)
        print(' done *')

    def wait_finalValue(self, sensor, value):
        print('wait until sensor: {} == value {}'.format(sensor, value))
        while status_finalValue(sensor) != value:
            time.sleep(1)
        print(' {} done *'.format(value))  

    #Simplified track table functions
    

if __name__ == '__main__':
   print("main")
