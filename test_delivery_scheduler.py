import logging
import pytest

from components.delivery.delivery_scheduler import calc_header_list, calc_body_row,\
     create_output_buffer, calc_list_partner_id, strip_file_path, calculate_sub_file_list

logger = logging.getLogger('custom_log_stat')
logger.setLevel(logging.DEBUG)


def test_calc_header_list():
    content_dict = {'fname': 'Avi', 'lname': 'Patil'}
    header_row = calc_header_list(content_dict)

    assert header_row == ['fname', 'lname']


def test_calc_body_row():
    content_dict = {'fname': 'Avi', 'lname': 'Patil'}
    body_row = calc_body_row(content_dict)

    assert body_row == ['Avi', 'Patil']


def test_create_output_buffer():
    body_list = ['Siju', 'Abraham']
    output_buffer = [['fname', 'lname'], ['Avi', 'Patil']]
    output_buffer = create_output_buffer(body_list, output_buffer)

    assert output_buffer == [['fname', 'lname'], ['Avi', 'Patil'], ['Siju', 'Abraham']]


def test_create_output_buffer():
    body_list = ['Chad', 'Feigenbutz']
    head_list = ['fname', 'lname']
    output_buffer = []

    output_buffer = create_output_buffer(body_list, output_buffer, head_list)
    assert output_buffer == [['fname', 'lname'], ['Chad', 'Feigenbutz']]


def test_strip_file_path():
    s_list = ['inbox/partner-234/xyz.txt', 'inbox/partner-456/abc.txt']
    transformed_list = strip_file_path(s_list)
    assert transformed_list == ['/partner-234/xyz.txt', '/partner-456/abc.txt']

def test_calculate_sub_file_list():
    s_list = ['inbox/partner-234/xyz.txt', 'inbox/partner-456/abc.txt', 'inbox/partner-456/pqr.txt']
    partner_specific_list = calculate_sub_file_list(s_list, 'partner-456')
    assert partner_specific_list == ['inbox/partner-456/abc.txt', 'inbox/partner-456/pqr.txt']
