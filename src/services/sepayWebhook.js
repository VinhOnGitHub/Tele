const config = require('../config');
const orderService = require('./orderService');
const productService = require('./productService');
const { deliverOrder } = require('../handlers/paymentConfirm');
const { setAdminState } = require('../handlers/adminActions');

const PAYMENT_CODE_REGEX = /\bNAP[A-Z0-9]{8}\b/i;

function extractPaymentCode(payload = {}) {
    const directCode = String(payload.code || '').trim().toUpperCase();
    if (directCode) return directCode;

    const content = String(payload.content || '');
    const match = content.match(PAYMENT_CODE_REGEX);
    return match ? match[0].toUpperCase() : null;
}

function isExpectedAccount(accountNumber = '') {
    const acc = String(accountNumber).trim();
    if (!acc) return false;

    if (acc === String(config.BANK.ACCOUNT).trim()) return true;
    if (config.BANK2 && acc === String(config.BANK2.ACCOUNT).trim()) return true;

    return false;
}

async function safeSend(bot, chatId, text) {
    try {
        await bot.telegram.sendMessage(chatId, text, { parse_mode: 'HTML' });
    } catch (err) {
        console.error(`Failed to send message to ${chatId}:`, err.message);
    }
}

async function handleSePayWebhook(bot, req, res) {
    try {
        const authHeader = (req.get('authorization') || '').trim();

        if (config.SEPAY_API_KEY && authHeader !== `Apikey ${config.SEPAY_API_KEY}`) {
            return res.status(401).json({ success: false, message: 'Unauthorized' });
        }

        const payload = req.body || {};

        if (payload.transferType !== 'in') {
            return res.status(200).json({ success: true, ignored: 'not_incoming_transfer' });
        }

        if (!isExpectedAccount(payload.accountNumber)) {
            return res.status(200).json({ success: true, ignored: 'wrong_bank_account' });
        }

        const paymentCode = extractPaymentCode(payload);
        if (!paymentCode) {
            return res.status(200).json({ success: true, ignored: 'no_payment_code' });
        }

        const order = orderService.getByPaymentCode(paymentCode);
        if (!order) {
            return res.status(200).json({
                success: true,
                ignored: 'order_not_found',
                paymentCode,
            });
        }

        // Chỉ bỏ qua khi đơn đã xong hẳn hoặc đã hủy
        if (!['pending', 'paid'].includes(order.status)) {
            return res.status(200).json({
                success: true,
                ignored: 'already_processed',
                orderId: order.id,
                status: order.status,
            });
        }

        const transferAmount = Number(payload.transferAmount || 0);

        if (transferAmount < Number(order.total_price || 0)) {
            await safeSend(
                bot,
                config.ADMIN_ID,
                `⚠️ Giao dịch chưa đủ tiền cho đơn #${order.id}\n` +
                `Mã CK: <code>${paymentCode}</code>\n` +
                `Cần: <b>${order.total_price}</b>\n` +
                `Nhận: <b>${transferAmount}</b>`
            );

            return res.status(200).json({
                success: true,
                ignored: 'insufficient_amount',
                orderId: order.id,
            });
        }

        const realStock = productService.getAvailableStock(order.product_id, order.quantity);

        // Có đủ kho thật => đánh dấu paid trước, rồi mới giao tự động
        if (realStock.length >= order.quantity) {
        if (order.status === 'pending') {
            const paidResult = orderService.markPaid(order.id);

            if (!paidResult.success) {
            await safeSend(
                bot,
                config.ADMIN_ID,
                `❌ Không thể chuyển đơn #${order.id} sang paid trước khi auto-delivery\n` +
                `Mã CK: \`${paymentCode}\`\n` +
                `Lỗi: ${paidResult.error || 'Unknown error'}`
            );

            return res.status(500).json({
                success: false,
                error: 'mark_paid_failed',
                orderId: order.id,
                message: paidResult.error || 'Cannot mark order as paid',
            });
            }
        }

        const result = await deliverOrder(bot, order.id);

        if (!result.success) {
            await safeSend(
            bot,
            config.ADMIN_ID,
            `❌ Auto-delivery lỗi cho đơn #${order.id}\n` +
            `Mã CK: \`${paymentCode}\`\n` +
            `Lỗi: ${result.error || 'Unknown error'}`
            );

            return res.status(500).json({
            success: false,
            error: 'delivery_failed',
            orderId: order.id,
            message: result.error || 'Auto delivery failed',
            });
        }

        await safeSend(
            bot,
            config.ADMIN_ID,
            `✅ SePay xác nhận thanh toán thành công và bot đã giao hàng tự động\n` +
            `Đơn #${order.id}\n` +
            `Mã CK: \`${paymentCode}\`\n` +
            `Số tiền: ${transferAmount}\n` +
            `Ref: \`${payload.referenceCode || ''}\``
        );

        return res.status(200).json({
            success: true,
            delivered: true,
            orderId: order.id,
        });
        }

        // Không đủ kho => chuyển sang paid để admin giao tay
        if (order.status === 'pending') {
            orderService.markPaid(order.id);
        }

        setAdminState(config.ADMIN_ID, {
            action: 'deliver_order',
            orderId: order.id,
            userId: order.user_id,
            productName: order.product_name,
            quantity: order.quantity,
        });

        await safeSend(
            bot,
            config.ADMIN_ID,
            `💰 SePay đã xác nhận thanh toán cho đơn #${order.id}, nhưng kho bot chưa đủ để auto giao.\n\n` +
            `Sản phẩm: <b>${order.product_name}</b>\n` +
            `Số lượng: <b>${order.quantity}</b>\n` +
            `Mã CK: <code>${paymentCode}</code>\n\n` +
            `➡️ Hãy gửi ngay thông tin acc theo từng dòng, ví dụ:\n` +
            `<code>mail1|pass1|ghi_chu</code>\n` +
            `<code>mail2|pass2|ghi_chu</code>\n\n` +
            `Gõ /cancel để hủy thao tác.`
        );

        await safeSend(
            bot,
            order.user_id,
            `✅ Shop đã nhận thanh toán cho đơn #${order.id}.\n` +
            `Đơn của bạn đang chờ giao thủ công vì sản phẩm hiện chưa có sẵn trong kho tự động.`
        );

        return res.status(200).json({
            success: true,
            paid: true,
            manual_delivery_required: true,
            orderId: order.id,
        });
    } catch (err) {
        console.error('SePay webhook error:', err);
        return res.status(500).json({ success: false, message: err.message });
    }
}

module.exports = { handleSePayWebhook };