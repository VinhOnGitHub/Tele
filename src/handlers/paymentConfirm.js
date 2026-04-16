const orderService = require('../services/orderService');
const productService = require('../services/productService');
const messages = require('../utils/messages');
const { postDeliveryKeyboard } = require('../utils/keyboard');

module.exports = (bot) => {
  bot.action(/^check_paid_(\d+)$/, async (ctx) => {
    const orderId = parseInt(ctx.match[1], 10);
    const order = orderService.getById(orderId);

    if (!order) {
      return ctx.answerCbQuery('❌ Đơn hàng không tồn tại');
    }

    if (order.status === 'delivered') {
      return ctx.answerCbQuery('✅ Đơn hàng đã được giao');
    }

    if (order.status === 'cancelled') {
      return ctx.answerCbQuery('❌ Đơn hàng đã bị hủy');
    }

    ctx.answerCbQuery();
    ctx.replyWithHTML(messages.paymentPending);
  });

  bot.action('data_main', (ctx) => {
    ctx.answerCbQuery();
    ctx.reply(' Tính năng đang phát triển...');
  });

  bot.action('buy_again', (ctx) => {
    ctx.answerCbQuery();
    const products = productService.getAll();
    const { productListKeyboard } = require('../utils/keyboard');
    ctx.reply(messages.productHeader, productListKeyboard(products));
  });
};

async function deliverOrder(bot, orderId) {
  const result = orderService.confirmAndDeliver(orderId);
  if (!result.success) {
    return result;
  }

  const order = result.order;
  const product = productService.getById(order.product_id);

  try {
    // Gửi ACC trước
    await bot.telegram.sendMessage(
      order.user_id,
      messages.orderSuccess(product, order.quantity, result.accounts),
      {
        parse_mode: 'HTML',
        ...postDeliveryKeyboard(),
      }
    );

    // Gửi thông báo ngắn sau, lỗi cũng không ảnh hưởng delivery chính
    try {
      await bot.telegram.sendMessage(
        order.user_id,
        messages.orderSuccessNotify(order.quantity),
        { parse_mode: 'HTML' }
      );
    } catch (notifyErr) {
      console.warn(`Notify failed for ${order.user_id}:`, notifyErr.message);
    }

    return result;
  } catch (err) {
    console.error(`Delivery failed for ${order.user_id}:`, err.message);

    try {
      orderService.rollbackDelivery(orderId, result.stockIds || []);
    } catch (rollbackErr) {
      console.error('Rollback delivery failed:', rollbackErr.message);
    }

    return {
      success: false,
      error: `Gửi tài khoản cho khách thất bại: ${err.message}`,
    };
  }
}

module.exports.deliverOrder = deliverOrder;