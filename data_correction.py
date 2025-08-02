from openerp import api
from openerp.osv import fields, osv
from openerp.tools.translate import _
from datetime import date, time, timedelta, datetime



class data_correction(osv.osv):
    _name = "data.correction"
    _description = "Employee Category"
    _columns = {
        'start_date': fields.date("Start Date"),
        'end_date': fields.date("End Date"),
        'opd_count': fields.integer("No of OPD"),
        'total_amount': fields.integer("Total amount"),
        'state': fields.selection(
            [('confirmed', 'Confirmed'), ('cancelled', 'Cancelled')],
            'Status', default='confirmed', readonly=True),
        'opd_ids': fields.text("OPD IDs"),
        'move_ids': fields.text("Move IDs"),
        'not_move_ids':fields.text("No journal")
    }

    def create(self, cr, uid, vals, context=None):
        start_date = vals.get('start_date')
        end_date = vals.get('end_date')

        # if start_date and end_date:
        #     cr.execute("""
        #             SELECT id FROM data_correction
        #             WHERE NOT (%s > end_date OR %s < start_date)
        #             LIMIT 1
        #         """, (start_date, end_date))
        #     existing = cr.fetchone()
        #
        #     if existing:
        #         raise osv.except_osv(
        #             'Date Range Overlap',
        #             'A record already exists that overlaps with this date range.'
        #         )

        return super(data_correction, self).create(cr, uid, vals, context=context)


    def opd_data(self,cr,uid,ids,context=None):
            no_move_ids=[]
            data_obj=self.browse(cr, uid, ids, context)
            start_date = data_obj.start_date
            end_date = data_obj.end_date
            cr.execute("""
                WITH daily_counts AS (
                    SELECT date, COUNT(*) AS total_tickets, CEIL(COUNT(*) * 0.3) AS sample_size
                    FROM opd_ticket
                    WHERE date BETWEEN %s AND %s AND total=300 AND state='confirmed' AND with_doctor_total=0 AND 
                    GROUP BY date
                ),
                numbered_tickets AS (
                    SELECT ot.*, ROW_NUMBER() OVER (PARTITION BY ot.date ORDER BY RANDOM()) AS rn
                    FROM opd_ticket ot
                    WHERE ot.date BETWEEN %s AND %s AND total=300 AND state='confirmed'
                )
                SELECT nt.id
                FROM numbered_tickets nt
                JOIN daily_counts dc ON nt.date = dc.date
                WHERE nt.rn <= dc.sample_size
            """,(start_date,end_date,start_date,end_date))
            opd_ticket_ids = cr.fetchall()
            data_obj.opd_ids=opd_ticket_ids
            for opd_ticket in opd_ticket_ids:
                opd_id = opd_ticket[0]  # Assuming `id` is the first column in your SELECT

                # Now execute your UPDATE query
                cr.execute("""
                    UPDATE opd_ticket
                    SET total = 0
                    WHERE id = %s
                """, (opd_id,))

                cr.execute("""
                        UPDATE opd_ticket_line
                        SET name = 65, price = 0, total_amount=0
                        WHERE opd_ticket_id = %s
                    """, (opd_id,))

                # Step 5: Update the total on the main ticket
                cr.execute("""
                        UPDATE opd_ticket
                        SET total = 0
                        WHERE id = %s
                    """, (opd_id,))

                # 3. Now, find the associated account.move by matching opd_ticket.name to account_move.ref
                cr.execute("""
                        SELECT id, ref
                        FROM account_move
                        WHERE ref = (
                            SELECT name
                            FROM opd_ticket
                            WHERE id = %s
                        )
                    """, (opd_id,))
                move = cr.fetchone()
                try:
                    move_str = str(move[0])
                    data_obj.move_ids = (data_obj.move_ids or '') + ',' + move_str
                except:
                    no_move_ids.append(opd_id)

                if move:
                    move_id = move[0]  # This is the account_move.id
                    opd_name = move[1]  # This is the opd_ticket.name (just for info)

                    # 4. Update account_move_line: credit lines
                    cr.execute("""
                            UPDATE account_move_line
                            SET credit = 0,
                                name = 'Eye Camp',
                                account_id = 6100
                            WHERE credit > 0
                              AND move_id = %s
                        """, (move_id,))

                    # 5. Update account_move_line: debit lines
                    cr.execute("""
                            UPDATE account_move_line
                            SET debit = 0, name=%s
                            WHERE debit > 0
                              AND move_id = %s
                        """, (opd_name,move_id,))
                    cr.commit()
                    move_obj=self.pool['account.move'].browse(cr, uid, move_id, context=None)
                    move_obj.button_cancel()
                    move_obj.button_validate()