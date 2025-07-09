from openerp import api
from openerp.osv import fields, osv
from openerp.tools.translate import _
from datetime import date, time, timedelta, datetime
import logging
_logger = logging.getLogger(__name__)



class ad_correction(osv.osv):
    _name = "ad.correction"
    _description = "Adm Correction"
    _columns = {
        'start_date': fields.date("Start Date"),
        'end_date': fields.date("End Date"),
        'from_adjust_item': fields.many2one("examination.entry","From Adjust Item"),
        'to_adjust_item': fields.many2one("examination.entry","To Adjust Item"),
        'from_adjust_item_rate':fields.float("Exisiting rate of from item",required=True),
        'to_adjust_item_rate':fields.float("Exisiting rate of to item",required=True),
        'percent':fields.float("Percent"),
        'section': fields.selection(
            [('bill.register', 'Bill Register'), ('leih.admission', 'Admission')],
            'Status',  readonly=True),
        'date': fields.boolean("Date"),
        'move_ids': fields.text("Move IDs"),
        'not_move_ids':fields.text("No journal")
    }

    def create(self, cr, uid, vals, context=None):
        start_date = vals.get('start_date')
        end_date = vals.get('end_date')
        from_adjust_item=vals.get('from_adjust_item')
        if start_date and end_date:
            cr.execute("""
                    SELECT id FROM ad_correction
                    WHERE from_adjust_item in (%s) AND NOT (%s > end_date OR %s < start_date)
                    LIMIT 1
                """, (from_adjust_item,start_date, end_date))
            existing = cr.fetchone()

            if existing:
                raise osv.except_osv(
                    'Date Range Overlap',
                    'A record already exists that overlaps with this date range.'
                )

        return super(ad_correction, self).create(cr, uid, vals, context=context)


    def money_receipt_correction(self, cr, uid, mr_ids, difference_amount, context=None):
        mr = self.pool.get('leih.money.receipt')

        if len(mr_ids) > 2:
            raise Exception("Too many money receipt, skipping correction for this item.")

        money_receipt_adjusted=False
        all_mr_amount=[]
        for mr_id in mr_ids:
            mr_obj = mr.browse(cr, uid, mr_id, context=context)
            all_mr_amount.append(mr_obj.amount)

        if all(amount > difference_amount for amount in all_mr_amount):
            raise Exception("both mr is grater than difference amount")


        for mr_id in mr_ids:
            mr_obj = mr.browse(cr, uid, mr_id, context=context)
            # condition of difference rate
            mr_amount = mr_obj.amount
            if mr_amount > difference_amount:
                cr.execute("""
                                UPDATE leih_money_receipt 
                                SET amount = amount - %s
                                WHERE id = %s
                            """, (difference_amount, mr_obj.id))
                money_receipt_adjusted=True
                break
        return money_receipt_adjusted


    def account_move_correction(self, cr, uid, journal_ids,data_obj,bill_ids,difference_amount, context=None):
        jr = self.pool.get('account.move')

        if len(journal_ids) > 2:
            raise Exception("Too many journal entries, skipping correction for this item.")

        for journal in sorted(journal_ids):
            journal_obj = jr.browse(cr, uid, journal, context=context)
            move_line = journal_obj.line_id

            for line_id in move_line:

                if line_id.account_id.id == data_obj.from_adjust_item.accounts_id.id:
                    cr.execute("""
                                    UPDATE account_move_line
                                    SET credit = %s,name = %s,account_id = %s
                                    WHERE id = %s
                                """, (data_obj.to_adjust_item_rate, data_obj.to_adjust_item.name,
                                      data_obj.to_adjust_item.accounts_id.id, line_id.id))
                if line_id.account_id.id == 6 and line_id.debit > difference_amount:
                    cash_amount = line_id.debit - difference_amount
                    cr.execute(""" UPDATE account_move_line SET debit = %s
                                    WHERE id = %s """,
                               (cash_amount, line_id.id))

                if line_id.account_id.id == 195 and line_id.debit > difference_amount:
                    cash_amount = line_id.debit - difference_amount
                    cr.execute(""" UPDATE account_move_line SET debit = %s
                                    WHERE id = %s """,
                               (cash_amount, line_id.id))
                if line_id.account_id.id == 195 and line_id.credit > difference_amount:
                    cash_amount = line_id.credit - difference_amount
                    cr.execute(""" UPDATE account_move_line SET credit = %s
                                    WHERE id = %s """,
                               (cash_amount, line_id.id))


            jr.button_cancel(cr, uid, [journal], context=context)
            jr.button_validate(cr, uid, [journal], context=context)




    def update_ad_line_data(self,cr,uid,ids,context=None):
        no_move_ids = []
        data_obj = self.browse(cr, uid, ids, context)
        start_date=data_obj.start_date
        end_date=data_obj.end_date
        from_adjust_item=data_obj.from_adjust_item.id
        to_adjust_item=data_obj.to_adjust_item.id
        percent=data_obj.percent


        cr.execute("""
            WITH eligible_bills AS (
                SELECT la.id
                FROM leih_admission la
                WHERE la.date BETWEEN %s AND %s
                  AND la.doctors_discounts = 0
                  AND la.other_discount = 0
                  AND la.due = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM leih_admission_line lal2
                      WHERE lal2.leih_admission_id = la.id AND lal2.name = %s
                  )
            ),
            matching_lines AS (
                SELECT 
                    lal.id AS line_id,
                    lal.leih_admission_id,
                    la.date::date AS bill_date,
                    ROW_NUMBER() OVER (PARTITION BY la.date::date ORDER BY RANDOM()) AS rn,
                    COUNT(*) OVER (PARTITION BY la.date::date) AS total_per_date
                FROM leih_admission_line lal
                JOIN leih_admission la ON lal.leih_admission_id = la.id
                WHERE la.id IN (SELECT id FROM eligible_bills)
                  AND lal.name = %s
            )
            SELECT 
                line_id, 
                leih_admission_id 
            FROM matching_lines
            WHERE rn <= CEIL(total_per_date * %s)
        """, (start_date, end_date, to_adjust_item, from_adjust_item,percent))

        results = cr.fetchall()
        # import pdb;pdb.set_trace()

        line_ids = []
        all_bill_ids = list(set(row[1] for row in results))
        # all_bill_ids = [19821]
        admission_obj = self.pool.get('leih.admission')
        admission_line_obj = self.pool.get('leih.admission.line')
        mr = self.pool.get('leih.money.receipt')
        jr = self.pool.get('account.move')
        bill_journal_relation=self.pool.get('admission.payment.line')
        bill_ids=[]

        for item in all_bill_ids:
            try:
                bill_adjusted=False
                savepoint_name = "sp_%s" % str(item)
                cr.execute("SAVEPOINT %s" % savepoint_name)
                admission_line = self.pool.get('leih.admission.line').search(cr, uid, [('leih_admission_id', '=', item)],
                                                                             context=context)
                for adm_line_id in admission_line:
                    admission_line_id = admission_line_obj.browse(cr, uid, adm_line_id, context=context)


                    if admission_line_id.name.id == from_adjust_item:
                        from_item_rate=admission_line_id.price
                        to_item_rate = data_obj.to_adjust_item_rate
                        difference_amount = from_item_rate - to_item_rate

                        mr_ids = self.pool.get('leih.money.receipt').search(cr, uid, [('admission_id', '=', item)],
                                                                            context=context)
                        abc = self.money_receipt_correction(cr, uid, mr_ids, difference_amount, context=context)
                        # import pdb;pdb.set_trace()
                        if abc == True:
                            bill_ids.append(item)
                            bill_adjusted=True

                        if bill_adjusted == True:
                            cr.execute("""
                                                  UPDATE leih_admission_line
                                                    SET name = %s, department=%s, price=%s, total_amount=%s
                                                    WHERE id = %s
                                                       """,
                                       (to_adjust_item, data_obj.to_adjust_item.department.name, data_obj.to_adjust_item_rate,
                                        data_obj.to_adjust_item_rate, admission_line_id.id))


                if bill_adjusted == True:
                    admission_id=admission_obj.browse(cr, uid, item, context=context)
                    cr.execute("""
                          UPDATE leih_admission SET total = total - %s,total_without_discount = total_without_discount - %s,grand_total = grand_total - %s,
                             paid = paid - %s WHERE id = %s
                                """, (difference_amount, difference_amount, difference_amount, difference_amount, item))




                    bill_journal_ids = bill_journal_relation.search(cr, uid, [('admission_payment_line_id', '=', item)], context=context)
                    for bji in bill_journal_ids:
                        bji_obj = bill_journal_relation.browse(cr, uid, bji, context=context)
                        if bji_obj.amount>difference_amount:
                            cr.execute("""
                                UPDATE admission_payment_line SET amount = amount - %s WHERE id = %s
                                """, (difference_amount, bji_obj.id))
                            break



                    #check journal
                    journal_ids = self.pool.get('account.move').search(cr, uid, [('ref', 'ilike', admission_id.name)],
                                                                            context=context)

                    self.account_move_correction(cr,uid,journal_ids,data_obj,bill_ids,difference_amount,context)


                    data_obj.move_ids=bill_ids
            except Exception as e:
                _logger.warning("Skipping admission ID %s due to error: %s", item, str(e))
                cr.execute("ROLLBACK TO SAVEPOINT %s" % savepoint_name)

                # rollback changes for just this item


