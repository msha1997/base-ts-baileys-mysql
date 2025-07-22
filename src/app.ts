import https from 'https'
import dotenv from 'dotenv'
import { join } from 'path'

import { createBot, createProvider, createFlow, addKeyword, utils } from '@builderbot/bot'
import { BaileysProvider as Provider } from '@builderbot/provider-baileys'
import { MysqlAdapter as Database } from './MysqlAdapter'

dotenv.config({ path: '.env' })
const PORT = process.env.PORT ?? 3008

const options = {
    hostname: process.env.APP_URL || "",
    port: 443,
    path: "/",
    method: "GET",
};

function generateRandomNumber(min: number, max: number) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pingServer() {
    const req = https.request(options, (res) => {
        if (res.statusCode === 200) {
            console.log("Ping successful at", new Date().toISOString());
        } else {
            console.error("Ping failed with status:", res.statusCode);
        }
        res.on("data", () => { });
    });

    req.on("error", (e) => {
        console.error("Error pinging server:", e);
    });

    req.end();
}


const discordFlow = addKeyword<Provider, Database>('doc').addAnswer(
    ['You can see the documentation here', '📄 https://builderbot.app/docs \n', 'Do you want to continue? *yes*'].join(
        '\n'
    ),
    { capture: true },
    async (ctx, { gotoFlow, flowDynamic }) => {
        if (ctx.body.toLocaleLowerCase().includes('yes')) {
            return gotoFlow(registerFlow)
        }
        await flowDynamic('Thanks!')
        return
    }
)

const welcomeFlow = addKeyword<Provider, Database>(['hi', 'hello', 'hola'])
    .addAnswer(`🙌 Hello welcome to this *Chatbot*`)
    .addAnswer(
        [
            'I share with you the following links of interest about the project',
            '👉 *doc* to view the documentation',
        ].join('\n'),
        { delay: 800, capture: true },
        async (ctx, { fallBack }) => {
            if (!ctx.body.toLocaleLowerCase().includes('doc')) {
                return fallBack('You should type *doc*')
            }
            return
        },
        [discordFlow]
    )

const registerFlow = addKeyword<Provider, Database>(utils.setEvent('REGISTER_FLOW'))
    .addAnswer(`What is your name?`, { capture: true }, async (ctx, { state }) => {
        await state.update({ name: ctx.body })
    })
    .addAnswer('What is your age?', { capture: true }, async (ctx, { state }) => {
        await state.update({ age: ctx.body })
    })
    .addAction(async (_, { flowDynamic, state }) => {
        await flowDynamic(`${state.get('name')}, thanks for your information!: Your age: ${state.get('age')}`)
    })

const fullSamplesFlow = addKeyword<Provider, Database>(['samples', utils.setEvent('SAMPLES')])
    .addAnswer(`💪 I'll send you a lot files...`)
    .addAnswer(`Send image from Local`, { media: join(process.cwd(), 'assets', 'sample.png') })
    .addAnswer(`Send video from URL`, {
        media: 'https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExYTJ0ZGdjd2syeXAwMjQ4aWdkcW04OWlqcXI3Ynh1ODkwZ25zZWZ1dCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/LCohAb657pSdHv0Q5h/giphy.mp4',
    })
    .addAnswer(`Send audio from URL`, { media: 'https://cdn.freesound.org/previews/728/728142_11861866-lq.mp3' })
    .addAnswer(`Send file from URL`, {
        media: 'https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf',
    })

const main = async () => {
    try {
        const adapterFlow = createFlow([welcomeFlow, registerFlow, fullSamplesFlow])

        const adapterProvider = createProvider(Provider)
        const adapterDB = new Database({
            host: process.env.MYSQL_DB_HOST,
            user: process.env.MYSQL_DB_USER,
            database: process.env.MYSQL_DB_NAME,
            password: process.env.MYSQL_DB_PASSWORD,
            port: Number(process.env.MYSQL_DB_PORT) || 3306,
        });

        const { handleCtx, httpServer } = await createBot({
            flow: adapterFlow,
            provider: adapterProvider,
            database: adapterDB,
        })

        adapterProvider.on('ready', () => {
            setInterval(pingServer, generateRandomNumber(30000, 50000));

            // Keep-alive mechanism
            const recipientJid = `${process.env.PHONE_NUMBER}@s.whatsapp.net`;
            setInterval(async () => {
                try {
                    await adapterProvider.sendText(recipientJid, 'Keep-alive message');
                    console.log('Keep-alive message sent at', new Date().toISOString());
                } catch (error) {
                    console.error('Error sending keep-alive message:', error);
                }
            }, generateRandomNumber(900000, 1800000));
        })

        adapterProvider.server.post(
            '/v1/messages',
            handleCtx(async (bot, req, res) => {
                const { number, message, urlMedia } = req.body
                await bot.sendMessage(number, message, { media: urlMedia ?? null })
                return res.end('sended')
            })
        )

        adapterProvider.server.post(
            '/v1/register',
            handleCtx(async (bot, req, res) => {
                const { number, name } = req.body
                await bot.dispatch('REGISTER_FLOW', { from: number, name })
                return res.end('trigger')
            })
        )

        adapterProvider.server.post(
            '/v1/samples',
            handleCtx(async (bot, req, res) => {
                const { number, name } = req.body
                await bot.dispatch('SAMPLES', { from: number, name })
                return res.end('trigger')
            })
        )

        adapterProvider.server.post(
            '/v1/blacklist',
            handleCtx(async (bot, req, res) => {
                const { number, intent } = req.body
                if (intent === 'remove') bot.blacklist.remove(number)
                if (intent === 'add') bot.blacklist.add(number)

                res.writeHead(200, { 'Content-Type': 'application/json' })
                return res.end(JSON.stringify({ status: 'ok', number, intent }))
            })
        )

        httpServer(+PORT)
    } catch (error) {
        console.error('Error in main:', error);
    }
}

main()