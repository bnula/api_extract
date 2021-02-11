import json

from db_class import DbOperations


class ParseFile:
    def __init__(self, file_location, db_server="dev_server", logger=None):
        self.__file=file_location
        self.__logger = logger
        self.__db_class = DbOperations(db_server, batch_size=500)
        misc_config_file = "D:/Python/config_files/misc_api_config.json"
        with open(misc_config_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            self.__com_code_list = data["commodity_codes"]

    @staticmethod
    def __create_line_metadata(
            item_metadata,
            line_metadata
    ):
        line_item_dictionary = {}
        for pair in item_metadata.items():
            key = pair[0]
            value = pair[1]
            if value is None:
                value = "NULL"
            line_item_dictionary[key] = value
        for pair in line_metadata.items():
            key = pair[0]
            value = pair[1]
            if value is None:
                value = "NULL"
            line_item_dictionary[key] = value
        return line_item_dictionary

    def __split_acc_list(
            self,
            line_item_metadata_dictionary,
            split_accounts
    ):
        split_acc_list = []
        for acc in split_accounts:
            self.__logger.info("Extract split acc data")
            acc_name = acc["Account"]["Name"]
            acc_id = acc["Account"]["Identifier"]
            bill_to_id = acc["BillTo"]["Identifier"]
            bill_to_name = acc["BillTo"]["Name"]
            bill_to_location = acc["BillTo"]["Address"]
            invoiced = acc["Invoiced"]
            if invoiced:
                invoiced_amount = invoiced["Amount"]
                invoiced_currency = invoiced["Currency"]
                invoiced_usd = invoiced["AmountUSD"]
            else:
                invoiced_amount = None
                invoiced_currency = None
                invoiced_usd = None
            split_acc_dictionary = {
                "AccountName": acc_name,
                "AccountID": acc_id,
                "BillToID": bill_to_id,
                "BillToName": bill_to_name,
                "BillToLocation": bill_to_location,
                "AccountInvoicedAmount": invoiced_amount,
                "AccountInvoicedInCurrency": invoiced_currency,
                "AccountInvoicedUSD": invoiced_usd
            }
            split_acc_dictionary = self.__create_line_metadata(
                item_metadata=line_item_metadata_dictionary,
                line_metadata=split_acc_dictionary
            )
            if split_acc_dictionary not in split_acc_list:
                self.__logger(f"add split acc line item")
                split_acc_list.append(split_acc_dictionary)
        return split_acc_list

    def __po_line_item_list(
            self,
            line_items,
            po_metadata_dictionary
    ):
        line_item_list = []
        for item in line_items:
            commodity_code = item["CommodityCode"]
            if commodity_code not in self.__com_code_list:
                continue
            else:
                line_number = item["NumberInCollection"]
                line_description = item["Description"]
                line_deliver_to = item["DeliverTo"]
                supplier = item["Supplier"]
                if supplier:
                    line_supplier_id = supplier["Identifier"]
                    line_supplier_name = supplier["Name"]
                    line_supplier_address = supplier["Address"]
                else:
                    line_supplier_id = None
                    line_supplier_name = None
                    line_supplier_address = None
                line_commodity_code_id = commodity_code["Identifier"]
                line_commodity_code_name = commodity_code["Name"]
                unit_price = item["Price"]["Amount"]
                unit_price_usd = item["Price"]["AmountUSD"]
                quantity = item["Quantity"]
                line_items_metadata_dictionary = {
                    "LineItemNumber": line_number,
                    "LineItemDescription": line_description,
                    "LineItemDeliverTo": line_deliver_to,
                    "LineItemSupplierID": line_supplier_id,
                    "LineItemSupplierName": line_supplier_name,
                    "LineItemSupplierAddress": line_supplier_address,
                    "CommodityCodeID": line_commodity_code_id,
                    "CommodityCodeName": line_commodity_code_name,
                    "Quantity": quantity,
                    "PricePerUnitLocal": unit_price,
                    "PricePerUInitUSD": unit_price_usd
                }
                split_accounts = item["SplitAccounts"]
                line_items_metadata_dictionary = self.__create_line_metadata(
                    item_metadata=po_metadata_dictionary,
                    line_metadata=line_items_metadata_dictionary
                )
                split_accounts_list = self.__split_acc_list(
                    line_item_metadata_dictionary=line_items_metadata_dictionary,
                    split_accounts=split_accounts
                )
                for dictionary in split_accounts_list:
                    line_item_list.append(dictionary)
        return line_item_list

    def purchase_order_metadata_extract(self, table_realm):
        self.__logger.info("========== Extract PO Metadata ===========")
        try:
            self.__logger.info("open PO file")
            with open(self.__file, "r", encoding="utf-8") as f:
                data = json.load(f)
                f.close()
            line_item_list = []
            self.__logger.info("go over each line in the file")
            for line in data:
                self.__logger.info("create dictionary for PO metadata")
                # create a dictionary for po metadata
                unique_name = line["RecordID"]
                order_id = line["OrderID"]
                order_name = line["Name"]
                order_date = line["CreatedOn"]
                order_status = line["Status"]
                vendor_id = line["Vendor"]["Identifier"]
                vendor_name = line["Vendor"]["Name"]
                invoiced = line["Invoiced"]
                if invoiced:
                    invoiced_amount = line["Invoiced"]["Amount"]
                    invoiced_currency = line["Invoiced"]["CurrencyCode"]
                    invoiced_in_usd = line["Invoiced"]["AmountUSD"]
                else:
                    invoiced_amount = None
                    invoiced_currency = None
                    invoiced_in_usd = None
                total_cost = line["Total"]["Amount"]
                total_cost_currency = line["Total"]["CurrencyCode"]
                total_cost_usd = line["Total"]["AmountUSD"]
                po_metadata = {
                    "UniqueName": unique_name,
                    "OrderID": order_id,
                    "OrderName": order_name,
                    "OrderCreationDate": order_date,
                    "OrderStatus": order_status,
                    "VendorID": vendor_id,
                    "VendorName": vendor_name,
                    "InvoicedLocalCurrency": invoiced_amount,
                    "InvoicedInCurrency": invoiced_currency,
                    "InvoicedUSD": invoiced_in_usd,
                    "TotalCostLocalCurrency": total_cost,
                    "TotalCostInCurrency": total_cost_currency,
                    "TotalCostUSD": total_cost_usd
                }
                line_items = line["LineItems"]
                line_items = self.__po_line_item_list(
                    line_items=line_items,
                    po_metadata_dictionary=po_metadata
                )
                for dictionary in line_items:
                    line_item_list.append(dictionary)
                self.__logger.info(f"List of unique lines for {order_id}")
            self.__db_class.insert_data(f"API_PO_{table_realm}", line_item_list)
        except Exception as e:
            self.__logger.error(f"Exception - {e}")

    def __invoice_line_item_list(
            self,
            line_items,
            invoice_metadata_dictionary
    ):
        line_item_list = []
        for item in line_items:
            commodity_code = item["CommodityCode"]
            if commodity_code not in self.__com_code_list:
                continue
            else:
                line_number = item["NumberInCollection"]
                line_description = item["Description"]
                line_deliver_to = item["DeliverTo"]
                cost_center = item["CostCenter"]
                if cost_center:
                    line_cost_center_id = cost_center["Identifier"]
                    line_cost_center_name = cost_center["Name"]
                else:
                    line_cost_center_id = None
                    line_cost_center_name = None
                supplier = item["Supplier"]
                if supplier:
                    line_supplier_id = supplier["Identifier"]
                    line_supplier_name = supplier["Name"]
                    line_supplier_address = supplier["Address"]
                else:
                    line_supplier_id = None
                    line_supplier_name = None
                    line_supplier_address = None
                line_commodity_code_id = commodity_code["Identifier"]
                line_commodity_code_name = commodity_code["Name"]
                line_items_metadata_dictionary = {
                    "LineItemNumber": line_number,
                    "LineItemDescription": line_description,
                    "LineItemDeliverTo": line_deliver_to,
                    "LineItemSupplierID": line_supplier_id,
                    "LineItemSupplierName": line_supplier_name,
                    "LineItemSupplierAddress": line_supplier_address,
                    "CommodityCodeID": line_commodity_code_id,
                    "CommodityCodeName": line_commodity_code_name,
                    "CostCenterID": line_cost_center_id,
                    "CostCenterName": line_cost_center_name
                }
                split_accounts = item["SplitAccounts"]
                line_items_metadata_dictionary = self.__create_line_metadata(
                    item_metadata=invoice_metadata_dictionary,
                    line_metadata=line_items_metadata_dictionary
                )
                split_accounts_list = self.__split_acc_list(
                    line_item_metadata_dictionary=line_items_metadata_dictionary,
                    split_accounts=split_accounts
                )
                for dictionary in split_accounts_list:
                    line_item_list.append(dictionary)
        return line_item_list

    def invoice_metadata_extract(self, table_realm):
        self.__logger.info("========== Extract Invoice Metadata ==========")
        try:
            self.__logger.info(f"open Invoice file")
            with open(self.__file, "r", encoding="utf-8") as f:
                data = json.load(f)
                f.close()
            line_item_list = []
            for line in data:
                order = line["Order"]
                if order is None:
                    continue
                else:
                    unique_name = line["RecordID"]
                    invoice_id = line["InvoiceID"]
                    invoice_name = line["Name"]
                    order_id = line["Order"]["Identifier"]
                    invoiced_total = line["Total"]["Amount"]
                    invoiced_currency = line["Total"]["CurrencyCode"]
                    invoiced_usd = line["Total"]["AmountUSD"]
                invoice_metadata = {
                    "UniqueName": unique_name,
                    "InvoiceID": invoice_id,
                    "InvoiceName": invoice_name,
                    "OrderID": order_id,
                    "TotalInvoiced": invoiced_total,
                    "TotalInvoicedInCurrency": invoiced_currency,
                    "TotalInvoicedUSD": invoiced_usd
                }
                line_items = line["LineItems"]
                if line_items is None:
                    continue
                else:
                    line_items = self.__invoice_line_item_list(
                        line_items=line_items,
                        invoice_metadata_dictionary=invoice_metadata
                    )
                    for dictionary in line_items:
                        line_item_list.append(dictionary)
            self.__db_class.insert_data(f"API_Invoice_{table_realm}", line_item_list)
        except Exception as e:
            self.__logger.error(f"Exception - {e}")

    def __request_line_items_list(
            self,
            line_items,
            request_metadata_dictionary
    ):
        line_items_list = []
        for item in line_items:
            commodity_code = item["CommodityCode"]
            if commodity_code is None:
                continue
            else:
                if commodity_code not in self.__com_code_list:
                    continue
                else:
                    line_number = item["NumberInCollection"]
                    commodity_code_id = commodity_code["Identifier"]
                    commodity_code_name = commodity_code["Name"]
                line_item_dictionary = {
                    "LineItemNumber": line_number,
                    "CommodityCodeID": commodity_code_id,
                    "CommodityCodeName": commodity_code_name
                }
                line_items = self.__create_line_metadata(
                    item_metadata=request_metadata_dictionary,
                    line_metadata=line_item_dictionary
                )
                for dictionary in line_items:
                    line_items_list.append(dictionary)
        return line_items_list

    def request_metadata_extract(self, table_realm):
        self.__logger.info("========== Extract Invoice Metadata ==========")
        try:
            self.__logger.info(f"open Invoice file")
            with open(self.__file, "r", encoding="utf-8") as f:
                data = json.load(f)
                f.close()
            line_item_list = []
            for line in data:
                order = line["OrderID"]
                if order == '':
                    continue
                else:
                    unique_name = line["RecordID"]
                    req_id = line["RequestID"]
                    req_name = line["Name"]
                    order_id = line["OrderID"]
                    requester = line["Requester"]
                request_metadata = {
                    "UniqueName": unique_name,
                    "RequestID": req_id,
                    "RequestName": req_name,
                    "OrderID": order_id,
                    "Requester": requester
                }
                line_items = line["LineItems"]
                if line_items is None:
                    continue
                else:
                    line_items = self.__request_line_items_list(
                        line_items=line_items,
                        request_metadata_dictionary=request_metadata
                    )
                    for dictionary in line_items:
                        line_item_list.append(dictionary)
            self.__db_class.insert_data(f"API_Requests_{table_realm}", line_item_list)
        except Exception as e:
            self.__logger.error(f"Exception - {e}")

    def attachments(self, table_realm):
        self.__logger.info("========== Attachments Metadata ==========")
        with open(self.__file, "r", "utf-8") as f:
            data = json.load(f)
            f.close()
        attachment_list = []
        for line in data:
            unique_name = None
            line_items = line["LineItems"]
            if not line_items:
                continue
            else:
                for item in line_items:
                    commodity_code = item["CommodityCode"]
                    if not commodity_code:
                        continue
                    else:
                        if commodity_code not in self.__com_code_list:
                            continue
                        else:
                            unique_name = item["RecordID"]
                if unique_name:
                    attachments = line["Attachments"]
                    if attachments:
                        for attachment in attachments:
                            attachment_name = attachment["FileName"]
                            attachment_unique_id = attachment["UniqueID"]
                            attachment_dictionary = {
                                "UniqueName": unique_name,
                                "AttachmentFileName": attachment_name,
                                "AttachmentID": attachment_unique_id
                            }
                            if attachment_dictionary not in attachment_list:
                                attachment_list.append(attachment_dictionary)
        self.__db_class.insert_data(f"API_Attachments_{table_realm}", attachment_list)