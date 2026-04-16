const config = require('../config');
const { customAlphabet } = require('nanoid');

const generateId = customAlphabet('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', 8);

const paymentService = {
    getBanks() {
        const banks = [config.BANK];
        if (config.BANK2) banks.push(config.BANK2);
        return banks;
    },

    getBank(index) {
        if (index === 1 && config.BANK2) return config.BANK2;
        return config.BANK;
    },

    generatePaymentCode() {
        return `NAP${generateId()}`;
    },

    generateQRUrl(amount, content, bank = null) {
        const b = bank || config.BANK;
        const encodedContent = encodeURIComponent(content);
        const encodedName = encodeURIComponent(b.ACCOUNT_NAME);

        return (
            `https://img.vietqr.io/image/${b.BIN}-${b.ACCOUNT}-compact2.png` +
            `?amount=${amount}` +
            `&addInfo=${encodedContent}` +
            `&accountName=${encodedName}`
        );
    },

    generatePayment(amount, bankIndex = 0) {
        const bank = this.getBank(bankIndex);
        const paymentCode = this.generatePaymentCode();
        const qrUrl = this.generateQRUrl(amount, paymentCode, bank);

        return {
            paymentCode,
            qrUrl,
            bankName: bank.NAME,
            accountNumber: bank.ACCOUNT,
            accountName: bank.ACCOUNT_NAME,
            amount,
        };
    },
};

module.exports = paymentService;