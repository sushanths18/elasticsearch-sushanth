#!/bin/python3.6
# -*- coding: utf-8 -*-

#-- Job Description:
#-----------------------------------------------------------------------------#
# Script marks all indices owned by CSE as "Read Only" after all writes are
# completed.
#-----------------------------------------------------------------------------#

import sys
import json
sys.path.append("/ctoc/app-common/python/log")
sys.path.append("/ctoc/app-common/python/alert")
sys.path.append("/ctoc/app-common/python/util")
sys.path.append("/ctoc/app-common/python/inventory")
import logger
import alert
import serversinventory
import esutil
import re
import datetime
from elasticsearch import ConnectionTimeout

# ========== Sets Up global variables ========== #

es = None
alert_doc = []

patterns = {
    "day_pattern": "-\d{4}\.\d{2}\.\d{2}",
    "month_pattern": "-\d{4}\.\d{2}\Z",
    "year_pattern": "-\d{4}\Z"
   }

date_formats = {
    "day_pattern": "%Y.%m.%d",
    "month_pattern": "%Y.%m",
    "year_pattern": "%Y"
    }

# ============== Sets up log files =============== #

log_location = '/ctoclogs/batch/admin_elkdataintegrity.log'
log = logger.get_logger(log_location,"elk_data_integrity")
log.info("---------- Started Run ----------")

# ========== Get cluster glip =========== #

def get_servers(cluster_name):
    try:
        servers = []
        cluster_port = serversinventory.get_cluster_port("cluster_name")
        if cluster_name in ('aws','azure'):
            cluster_nodes = serversinventory.get_servers_from_ansible_cmd("elk-"+ cluster_name +"-data")
            servers = ["https://" + node + ".usaa.com:" + str(cluster_port) for node in cluster_nodes]
        else:
            cluster_glip = serversinventory.get_cluster_glip(cluster_name)
            servers = ["https://" + cluster_glip]
    except Exception as err:
        log.critical("Exception occurred while getting the cluster glip of  " + cluster_name)
        log.exception(err)
    return servers

# ========== Mark indices Read-only ========== #

def read_only_indices(cluster_name,es,indices_required):
    try:
        for read_only_index in indices_required:
            # This API gets stats of that particular index
            index_stats = es.indices.stats(index=read_only_index)
            if index_stats is not None:
                # Before marking index read-only, check if it is not current index to make sure it is not being written to anymore
                if index_stats['_all']['total']['indexing']['index_total'] == 0 and index_stats['_all']['total']['indexing']['index_current'] == 0:
                    log.debug("Calling index read_only API on {} index: {}".format(cluster_name,read_only_index))
                    response = es.indices.put_settings(index=read_only_index, body={"index.blocks.read_only":False,"index.blocks.read_only_allow_delete":True})
                    if response['acknowledged'] != True:
                        log.debug("Unable to mark "+read_only_index+" as read_only. Please check the index settings once.")
    except ConnectionTimeout as connectionTimeout:
        # Index read-only API is expected to timeout as it may take more time if there are more number of indices
        log.debug("Connection timed out for index read-only-allow-delete API call for cluster: {}".format(cluster_name))
    except Exception as err:
        error_msg = "Exception occurred while marking read only indices for " + cluster_name
        log.error(error_msg)
        log.exception(err)
        critical_doc = {"Id" : cluster_name, "Error_msg": error_msg}
        alert_doc.append(critical_doc)

# ========== Get date pattern of indices ========== #

def get_date_pattern(index):
    for pattern in patterns.keys():
        if re.search(patterns[pattern], index):
            pattern_matched = pattern
            return pattern_matched

# ========== Get indices from each cluster  ========== #

def get_indices(cluster_name,es):
    indices_list = []
    try:
        cat_indices_resp = es.cat.indices(s='index',format='json')
        for index_details in cat_indices_resp:
            index = index_details["index"]
            if index.startswith("ls-"):
                # Get date pattern from index name
                date_pattern = get_date_pattern(index)
                if date_pattern is not None:
                    date_part = re.search(patterns[date_pattern], index).group(0)[1:]
                    index_date = datetime.datetime.strptime(date_part ,date_formats[date_pattern])
                    if date_pattern in ("day_pattern","date_quarter_pattern"):
                        # Check if index is older than 24 hours and add to the list if index type is Quarterdaily or daily.
                        date_diff_in_hours = (datetime.datetime.now() - index_date).total_seconds() /3600
                        if date_diff_in_hours >= 48 and date_diff_in_hours <= 72:
                            indices_list.append(index)
                    elif date_pattern is "month_pattern":
                        # Check if index is older than 31(highest number in a month) days and add to the list if index type is monthly.
                        date_diff_in_days = (datetime.datetime.now() - index_date).days
                        if date_diff_in_days >= 31 and date_diff_in_days <= 61:
                            indices_list.append(index)
                    elif date_pattern is "year_pattern":
                        # Check if index is older than 366(leap year) days and add to the list if index type is yearly.
                        date_diff_in_days = (datetime.datetime.now() - index_date).days
                        if date_diff_in_days >= 366 and date_diff_in_days <= 395:
                            indices_list.append(index)
    except Exception as err:
        error_msg = "Exception occurred while retreiving indices of " + cluster_name
        log.error(error_msg)
        log.exception(err)
        critical_doc = {"Id" : cluster_name, "Error_msg": error_msg}
        alert_doc.append(critical_doc)
    return indices_list

# ========== send alert for critical docs ========== #

def send_alert():
    global alert_doc
    if (alert_doc>0):
        try:
            alert.alert_critical_metrics(alert_doc,"elk_data_integrity")
        except Exception as err:
            log.error("Exception while sending alert for critical docs")
            log.exception(err)

# ========== merge older than 48 hours indices ========== #

def force_merge_get_indices(cluster_name,es):
    indices_list = []
    try:
        cat_indices_resp = es.cat.indices(s='index',format='json')
        for index_details in cat_indices_resp:
            index = index_details["index"]
            if index.startswith("ls-"):
                # Get date pattern from index name
                date_pattern = get_date_pattern(index)
                if date_pattern is not None:
                    date_part = re.search(patterns[date_pattern], index).group(0)[1:]
                    index_date = datetime.datetime.strptime(date_part ,date_formats[date_pattern])
                    if date_pattern in ("day_pattern","date_quarter_pattern"):
                        # Check if index is older than 24 hours and add to the list if index type is Quarterdaily or daily.
                        date_diff_in_hours = (datetime.datetime.now() - index_date).total_seconds() /3600
                        if date_diff_in_hours >= 48 and date_diff_in_hours <= 72:
                        #if date_diff_in_hours >= 48:
                            indices_list.append(index)
                            es.forcemerge(index)
                            #forcemerge(index=None, params=None, headers=None)
                    elif date_pattern is "month_pattern":
                        # Check if index is older than 31(highest number in a month) days and add to the list if index type is monthly.
                        date_diff_in_days = (datetime.datetime.now() - index_date).days
                        if date_diff_in_days >= 31 and date_diff_in_days <= 61:
                            indices_list_monthly.append(index)
                    elif date_pattern is "year_pattern":
                        # Check if index is older than 366(leap year) days and add to the list if index type is yearly.
                        date_diff_in_days = (datetime.datetime.now() - index_date).days
                        if date_diff_in_days >= 366 and date_diff_in_days <= 395:
                            indices_list_yearly.append(index)
    except Exception as err:
        error_msg = "Exception occurred while retreiving indices of " + cluster_name
        log.error(error_msg)
        log.exception(err)
        critical_doc = {"Id" : cluster_name, "Error_msg": error_msg}
        alert_doc.append(critical_doc)
    return indices_list

# ============== Main method ============== #

def main():
    # Get indices those needs to be marked read-only
    cluster_list = serversinventory.get_cluster_list()
    for cluster_name in cluster_list:
        # Get cluster host names to connect
        servers = get_servers(cluster_name)
        es = esutil.getES(servers)
        indices_required = get_indices(cluster_name,es)
        # Mark indices read-only
        read_only_indices(cluster_name, es, indices_required)

main()
log.info("---------- Finished Run ----------")

