from openerp import api
from openerp.osv import fields, osv
from openerp.tools.translate import _
from datetime import date, time, timedelta, datetime



class bill_correction(osv.osv):
    _name = "bill.correction"
    _description = "Bill Correction"
    _columns = {
        'start_date': fields.date("Start Date"),
        'end_date': fields.date("End Date"),
        'from_adjust_item': fields.many2one("examination.entry","From Adjust Item"),
        'to_adjust_item': fields.many2one("examination.entry","To Adjust Item"),
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
                    SELECT id FROM bill_correction
                    WHERE from_adjust_item in (%s) AND NOT (%s > end_date OR %s < start_date)
                    LIMIT 1
                """, (from_adjust_item,start_date, end_date))
            existing = cr.fetchone()

            if existing:
                raise osv.except_osv(
                    'Date Range Overlap',
                    'A record already exists that overlaps with this date range.'
                )

        return super(bill_correction, self).create(cr, uid, vals, context=context)


    def update_bill_line_data(self,cr,uid,ids,context=None):
        no_move_ids = []
        data_obj = self.browse(cr, uid, ids, context)
        start_date=data_obj.start_date
        end_date=data_obj.end_date
        from_adjust_item=data_obj.from_adjust_item.id
        to_adjust_item=data_obj.to_adjust_item.id
        percent=data_obj.percent

        cr.execute("""
            WITH eligible_bills AS (
                SELECT br.id
                FROM bill_register br
                WHERE br.date BETWEEN %s AND %s
                  AND br.doctors_discounts = 0
                  AND br.other_discount = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM bill_register_line brl2
                      WHERE brl2.bill_register_id = br.id AND brl2.name = %s
                  )
            ),
            matching_lines AS (
                SELECT 
                    brl.id AS line_id,
                    brl.bill_register_id,
                    br.date::date AS bill_date,
                    ROW_NUMBER() OVER (PARTITION BY br.date::date ORDER BY RANDOM()) AS rn,
                    COUNT(*) OVER (PARTITION BY br.date::date) AS total_per_date
                FROM bill_register_line brl
                JOIN bill_register br ON brl.bill_register_id = br.id
                WHERE br.id IN (SELECT id FROM eligible_bills)
                  AND brl.name = %s
            )
            SELECT 
                line_id, 
                bill_register_id 
            FROM matching_lines
            WHERE rn <= CEIL(total_per_date * %s)
        """, (start_date, end_date, to_adjust_item, from_adjust_item,percent))

        results = cr.fetchall()

        line_ids = []
        all_bill_ids = list(set(row[1] for row in results))


        cr.execute("""
                SELECT bill_id 
                FROM leih_money_receipt 
                WHERE bill_id IN %s
                GROUP BY bill_id
                HAVING COUNT(id) <= 1
            """, (tuple(all_bill_ids),))
        bill_ids = [row[0] for row in cr.fetchall()]
        for items in results:
            if items[1] in bill_ids:
                line_ids.append(items[0])

        # import pdb;pdb.set_trace()

        if line_ids:
            #count money receipt details
            cr.execute("""
                UPDATE bill_register_line
                SET name = %s, department=%s, price=%s, total_amount=%s
                WHERE id IN %s
            """, (to_adjust_item,data_obj.to_adjust_item.department.name,data_obj.to_adjust_item.rate,data_obj.to_adjust_item.rate, tuple(line_ids)))

        if bill_ids:
            from_item_rate=data_obj.from_adjust_item.rate
            to_item_rate=data_obj.to_adjust_item.rate
            difference_rate=from_item_rate-to_item_rate

            cr.execute("""
                UPDATE bill_register
                SET 
                    total = total - %s,total_without_discount = total_without_discount - %s,grand_total = grand_total - %s,
                    paid = paid - %s
                WHERE id IN %s
            """, (difference_rate,difference_rate,difference_rate,difference_rate,tuple(bill_ids)))


            #update money_receipt
            cr.execute("""
                UPDATE leih_money_receipt
                SET amount = amount - %s
                WHERE bill_id IN %s
            """, (difference_rate, tuple(bill_ids)))



            #update bill_register_payment
            cr.execute("""
                UPDATE bill_register_payment_line
                SET amount = amount - %s
                WHERE bill_register_payment_line_id IN %s
            """, (difference_rate, tuple(bill_ids)))

            #update journal Items
            cr.execute("""
                SELECT name FROM bill_register WHERE id IN %s
            """, (tuple(bill_ids),))
            register_names = [r[0] for r in cr.fetchall()]

            cr.execute("""
                SELECT id, ref FROM account_move
                WHERE ref IN %s
            """, (tuple(register_names),))
            move_data = cr.fetchall()
            move_ids = [r[0] for r in move_data]

            cr.execute("""
                UPDATE account_move_line
                SET credit = %s,name = %s,account_id = %s
                WHERE move_id IN %s AND account_id = %s
            """, (data_obj.to_adjust_item.rate, data_obj.to_adjust_item.name,data_obj.to_adjust_item.accounts_id.id, tuple(move_ids), data_obj.from_adjust_item.accounts_id.id))

            cr.execute("""
                UPDATE account_move_line
                SET debit = debit - %s
                WHERE move_id IN %s AND account_id = 6
            """, (difference_rate,tuple(move_ids)))

            move_obj = self.pool.get('account.move')
            for move_id in move_ids:
                move_obj.button_cancel(cr, uid, [move_id], context=context)
                try:
                    move_obj.button_validate(cr, uid, [move_id], context=context)
                except Exception:
                    import pdb;pdb.set_trace()

            data_obj.move_ids=bill_ids



        # line_ids = [row[0] for row in cr.fetchall()]
        # import pdb;pdb.set_trace()


        ## I am updateing the id , will update the amount also with kazi
        # if line_ids:
        #     for line_id in line_ids:
        #         cr.execute("""
        #             UPDATE bill_register_line
        #             SET examination_id = 26
        #             WHERE id = %s
        #         """, (line_id,))
        #         bill_id=line_id.bill_resigter_name.id
        #         self.update_bill_amount(bill_id)

            # include all line items of the bill

        #     update_lines
        #     AS(
        #         UPDATE
        #     bill_register_line
        #     SET
        #     name = %s
        #     WHERE
        #     id
        #     IN(SELECT
        #     line_id
        #     FROM
        #     random_sample)
        #     RETURNING
        #     bill_register_id
        #     )
        #     UPDATE
        #     bill_register
        #     SET
        #     total = total - 3000,
        #     paid = paid - 3000
        # WHERE
        # id
        # IN(SELECT
        # bill_register_id
        # FROM
        # update_lines);


# def update_bill_amount(self,cr, uid, ids, context=None):
#     cr.execute("""
#     WITH totals AS (
#         SELECT
#             br.id AS bill_id,
#             COALESCE(SUM(brl.total_amount), 0) AS total_sum
#         FROM bill_register br
#         LEFT JOIN bill_register_line brl ON brl.bill_register_id = br.id
#         WHERE br.id = %s
#         GROUP BY br.id
#     )
#     UPDATE bill_register br
#     SET
#         total = t.total_sum,
#         total_with_discount = (br.due - br.discount),
#         grand_total = t.total_sum,
#         paid = CASE
#                  WHEN br.paid > t.total_sum THEN br.paid
#                  ELSE br.paid
#                END,
#         due = CASE
#                 WHEN br.paid <= t.total_sum THEN t.total_sum - br.paid
#                 ELSE 0
#               END
#     FROM totals t
#     WHERE br.id = t.bill_id
#     """, (bill_id))
#
#
# def update_money_receipt_amount():
#     cr.execute("""
#         UPDATE leih_money_receipt
#         SET paid_amount = 300.00
#         WHERE bill_id = ANY(%s)
#     """, (bill_id,))
#
#
# def update_journal_amount():
#     cr.execute("""
#                 SELECT id, ref
#                 FROM account_move
#                 WHERE ref = (%s)
#                     """, (bill_register_id.name,))
#                 move = cr.fetchone()
#     move = cr.fetchone()
#
#     if move:
#             move_id = move[0]  # This is the account_move.id
#             opd_name = move[1]  # This is the opd_ticket.name (just for info)
#             paid_amount = 400.00 ## Here we will be changing the amount
#
#             # 4. Update account_move_line: credit lines
#             cr.execute("""
#                     UPDATE account_move_line
#                     SET credit = %s
#                     WHERE credit > 0
#                         AND move_id = %s
#                 """, (paid_amount,move_id,))
#
#             # 5. Update account_move_line: debit lines
#             cr.execute("""
#                     UPDATE account_move_line
#                     SET debit = %s
#                     WHERE debit > 0
#                         AND move_id = %s
#                 """, (paid_amount,move_id,))
#             cr.commit()
#             move_obj=self.pool['account.move'].browse(cr, uid, move_id, context=None)
#             move_obj.button_cancel()
#             move_obj.button_validate()