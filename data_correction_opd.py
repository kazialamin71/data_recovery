from openerp.osv import osv, fields
from openerp.tools.translate import _
from datetime import datetime

class data_correction_opd(osv.osv):
    _name = "data.correction.opd"
    _description = "OPD Ticket to Journal Entry Creator"

    _columns = {
        'ticket_names': fields.text("OPD Ticket Names (comma separated)"),
    }

    def opd_data_correction(self, cr, uid, ids, context=None):
        if context is None:
            context = {}
        for record in self.browse(cr, uid, ids, context=context):
            ticket_names = [x.strip() for x in record.ticket_names.split(",") if x.strip()]
            for name in ticket_names:
                opd = self.pool.get('opd.ticket').search(cr, uid, [('name', '=', name)], context=context)
                if not opd:
                    continue
                opd = self.pool.get('opd.ticket').browse(cr, uid, opd[0], context=context)

                # Check if already journal entry exists
                existing_move = self.pool.get('account.move').search(cr, uid, [('ref', '=', opd.name)], context=context)
                if existing_move:
                    continue

                period_obj = self.pool.get('account.period')
                period_id = period_obj.search(cr, uid, [('date_start', '<=', opd.date), ('date_stop', '>=', opd.date)], limit=1)
                if not period_id:
                    raise osv.except_osv(_('Error!'), _('No period found for date %s') % opd.date)

                move_vals = {
                    'journal_id': 2,
                    'ref': opd.name,
                    'date': opd.date,
                    'period_id': period_id[0],
                    'line_id': [],
                }

                # Debit line
                debit_line = (0, 0, {
                    'name': opd.name,
                    'account_id': 6,
                    'debit': opd.total,
                    'credit': 0.0,
                })

                # Credit line
                if not opd.opd_ticket_line_id:
                    raise osv.except_osv(_('Missing Data'), _('OPD Ticket %s has no line items') % opd.name)

                account_id = opd.opd_ticket_line_id[0].name.accounts_id.id
                if not account_id:
                    raise osv.except_osv(_('Missing Account'), _('Line "%s" of OPD Ticket "%s" has no account assigned.') % (opd.opd_ticket_line_id[0].name.name, opd.name))

                credit_line = (0, 0, {
                    'name': opd.name,
                    'account_id': account_id,
                    'debit': 0.0,
                    'credit': opd.total,
                })

                move_vals['line_id'] = [debit_line, credit_line]

                move_id = self.pool.get('account.move').create(cr, uid, move_vals, context=context)
                self.pool.get('account.move').post(cr, uid, [move_id], context=context)

        return True
